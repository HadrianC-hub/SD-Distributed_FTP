import socket
import threading
import json
import time
import uuid
from typing import Dict, List, Callable

class ClusterCommunication:
    def __init__(self, node_id: str, cluster_ips: List[str]):
        self.node_id = node_id
        self.cluster_ips = cluster_ips
        self.message_handlers = {}
        self.server_socket = None
        self.local_ip = self.get_local_ip()
        
    def get_local_ip(self):
        """Obtiene la IP local del contenedor"""
        try:
            # En Docker, el hostname es la IP del contenedor
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            return local_ip
        except Exception:
            try:
                # Fallback: método alternativo
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                local_ip = s.getsockname()[0]
                s.close()
                return local_ip
            except Exception:
                return '127.0.0.1'
  
    # --- MANEJO DEL SERVIDOR ---

    def start_server(self, port: int = 2123):
        """Inicia el servidor para escuchar mensajes de otros nodos"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Intentar bind en el puerto, si falla esperar y reintentar
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    self.server_socket.bind(('0.0.0.0', port))
                    break
                except OSError as e:
                    if "Address already in use" in str(e) and attempt < max_retries - 1:
                        print(f"[ERROR][CLUSTER] Puerto {port} en uso, reintentando en 2 segundos... ({attempt + 1}/{max_retries})")
                        time.sleep(2)
                    else:
                        print(f"[ERROR][CLUSTER] Error al conectar el socket de comunicaciones: {e}")

            self.server_socket.listen(10)

            print(f"[CLUSTER] Servidor de cluster escuchando en puerto {port}")

            while True:
                try:
                    client_socket, addr = self.server_socket.accept()
                    print(f"[CLUSTER] Conexión entrante de {addr[0]}:{addr[1]}")
                    threading.Thread(target=self.handle_client, args=(client_socket,), daemon=True).start()
                except Exception as e:
                    print(f"[ERROR][CLUSTER] Error aceptando conexión: {e}")
        except Exception as e:
            print(f"[ERROR][CLUSTER] Error crítico iniciando servidor: {e}")
            if port == 2123:
                print("[CLUSTER] Intentando puerto alternativo 2124...")
                self.start_server(2124)

    def handle_client(self, client_socket):
        """Maneja una conexión entrante de otro nodo con mejor manejo de JSON"""
        try:
            # Leer todos los datos disponibles
            data = b""
            client_socket.settimeout(5.0)  # Timeout para prevenir bloqueos
            
            while True:
                try:
                    chunk = client_socket.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                    
                    # Intentar parsear para ver si tenemos un JSON completo
                    try:
                        message = json.loads(data.decode())
                        break  # Si se puede parsear, tenemos el mensaje completo
                    except json.JSONDecodeError:
                        # Continuar recibiendo datos
                        continue
                except socket.timeout:
                    break
                except BlockingIOError:
                    break
            
            if not data:
                return
                
            try:
                message = json.loads(data.decode())
                self.process_message(message, client_socket)
            except json.JSONDecodeError as e:
                print(f"[ERROR][CLUSTER] Error decodificando JSON: {e}")
                print(f"[ERROR][CLUSTER] Datos recibidos (parcial): {data[:200]}...")
                client_socket.send(json.dumps({
                    'status': 'error', 
                    'message': f'Invalid JSON: {str(e)}'
                }).encode())
                
        except Exception as e:
            print(f"[ERROR][CLUSTER] Error manejando cliente: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass

    def process_message(self, message: Dict, client_socket):
        """Procesa un mensaje recibido y ejecuta el handler correspondiente"""
        msg_type = message.get('type')
        handler = self.message_handlers.get(msg_type)
        
        if handler:
            try:
                response = handler(message)
                if response:
                    payload = json.dumps(response).encode()
                    try:
                        print(f"[CLUSTER] Enviando respuesta a cliente: {response.get('status','?')} ({len(payload)} bytes)")
                    except Exception:
                        pass
                    client_socket.send(payload)
                    # Señalizar EOF al cliente para aumentar probabilidad de entrega inmediata
                    try:
                        client_socket.shutdown(socket.SHUT_WR)
                    except Exception:
                        pass
            except Exception as e:
                print(f"[ERROR][CLUSTER] Error en handler para {msg_type}: {e}")
                client_socket.send(json.dumps({'status': 'error', 'message': str(e)}).encode())
        else:
            print(f"[ERROR][CLUSTER] No hay handler para el tipo de mensaje: {msg_type}")

    def update_cluster_ips(self, new_ips: List[str]):
        """Actualiza la lista de IPs del cluster"""
        self.cluster_ips = new_ips

    # --- REGISTRO DE HANDLERS ---

    def register_handler(self, msg_type: str, handler: Callable):
        """Registra un handler para un tipo de mensaje específico"""
        self.message_handlers[msg_type] = handler

    # --- ENVIO DE MENSAJES ---

    def send_message(self, target_ip: str, message: Dict, expect_response: bool = True) -> Dict:
        """Envía un mensaje a un nodo específico con manejo robusto de red/JSON"""

        if target_ip == self.local_ip:
            return {'status': 'skipped', 'message': 'Self-send avoided'}

        max_retries = 2

        # Serializar una sola vez (no depende de la red)
        try:
            message_bytes = json.dumps(message, separators=(',', ':')).encode()
        except Exception as e:
            print(f"[ERROR][CLUSTER] Error serializando mensaje: {e}")
            return {'status': 'error', 'message': f'Serialization error: {str(e)}'}

        for attempt in range(max_retries):
            try:
                response_data = b""

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(15)
                    sock.connect((target_ip, 2123))

                    print(f"[CLUSTER] Conectado a {target_ip}:2123 (intento {attempt + 1}/{max_retries})")

                    sock.sendall(message_bytes)
                    print(f"[CLUSTER] Enviado {len(message_bytes)} bytes a {target_ip}")

                    if not expect_response:
                        return {'status': 'sent'}

                    sock.settimeout(15.0)

                    while True:
                        try:
                            chunk = sock.recv(4096)
                            if not chunk:
                                break

                            response_data += chunk

                            # Salir temprano si ya es JSON válido
                            try:
                                json.loads(response_data.decode())
                                break
                            except json.JSONDecodeError:
                                continue

                        except socket.timeout:
                            print(f"[ERROR][CLUSTER] Timeout recibiendo respuesta de {target_ip}")
                            break

                # ===== fuera del socket =====

                if not response_data:
                    print(f"[ERROR][CLUSTER] No se recibieron bytes desde {target_ip}")
                    return {'status': 'timeout', 'message': 'No response received'}

                # Intento normal de decodificación
                try:
                    return json.loads(response_data.decode())
                except json.JSONDecodeError as e:
                    # Intento de recuperación con raw_decode
                    try:
                        dec = json.JSONDecoder()
                        obj, _ = dec.raw_decode(response_data.decode(errors='ignore'))
                        print(f"[CLUSTER] Partial/extra data decodificada usada para respuesta desde {target_ip}")
                        return obj
                    except Exception:
                        print(
                            f"[ERROR][CLUSTER] Error decodificando respuesta de {target_ip}: {e} "
                            f"-- raw: {response_data[:200]!r}"
                        )
                        return {'status': 'error', 'message': f'Invalid JSON response: {str(e)}'}

            except socket.timeout:
                print(f"[ERROR][CLUSTER] Timeout enviando mensaje a {target_ip} (intento {attempt + 1}/{max_retries})")
                if attempt == max_retries - 1:
                    return {'status': 'timeout', 'message': f'Timeout after {max_retries} attempts'}

            except ConnectionRefusedError:
                print(f"[ERROR][CLUSTER] Conexión rechazada por {target_ip} (intento {attempt + 1}/{max_retries})")
                if attempt == max_retries - 1:
                    return {'status': 'error', 'message': 'Connection refused'}

            except Exception as e:
                print(f"[ERROR][CLUSTER] Error enviando mensaje a {target_ip} (intento {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    return {'status': 'error', 'message': str(e)}

            if attempt < max_retries - 1:
                time.sleep(1)

        return {'status': 'error', 'message': 'Max retries exceeded'}
    
    def broadcast_message(self, message: Dict, expect_responses: bool = True) -> Dict[str, Dict]:
        """Envía un mensaje a todos los nodos del cluster EXCEPTO a sí mismo"""

        base_message = message.copy()
        base_message['sender'] = self.node_id
        base_message['sender_ip'] = self.local_ip
        base_message['operation_id'] = str(uuid.uuid4())
        base_message['timestamp'] = time.time()

        responses: Dict[str, Dict] = {}
        lock = threading.Lock()

        def _send(ip: str):
            local_msg = base_message.copy()

            if ip == self.local_ip:
                with lock:
                    responses[ip] = {'status': 'skipped', 'message': 'Self-send avoided'}
                return

            print(f"[CLUSTER] Enviando mensaje a {ip}")
            resp = self.send_message(ip, local_msg, expect_responses)

            with lock:
                responses[ip] = resp

        threads = []
        for ip in self.cluster_ips:
            t = threading.Thread(target=_send, args=(ip,), daemon=True)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Reintento rápido para nodos problemáticos
        timed_out = [
            ip for ip, r in responses.items()
            if r is None or r.get('status') in ('timeout', 'error')
        ]

        if timed_out:
            print(f"[CLUSTER] Reintentos para nodos con respuestas problemáticas: {timed_out}")

            for ip in timed_out:
                if ip == self.local_ip:
                    continue

                try:
                    time.sleep(0.1)
                    print(f"[CLUSTER] Reintentando mensaje a {ip} (retry)")
                    responses[ip] = self.send_message(ip, base_message.copy(), expect_responses)
                except Exception as e:
                    print(f"[ERROR][CLUSTER] Error en retry a {ip}: {e}")

        return responses

# Singleton global
_cluster_comm_instance = None
def start_cluster_communication(node_id: str, cluster_ips: List[str]):
    global _cluster_comm_instance
    if _cluster_comm_instance is None:
        _cluster_comm_instance = ClusterCommunication(node_id, cluster_ips)
        threading.Thread(target=_cluster_comm_instance.start_server, daemon=True).start()
    return _cluster_comm_instance
