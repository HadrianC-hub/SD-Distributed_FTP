import socket
import threading
import json
import time
import uuid
from typing import Dict, List, Callable
from json import JSONDecoder

class ClusterCommunication:
    def __init__(self, node_id: str, cluster_ips: List[str]):
        self.node_id = node_id
        self.cluster_ips = cluster_ips
        self.message_handlers = {}
        self.server_socket = None
        self.pending_operations = {} 
        self.operation_timeout = 10
        self.local_ip = self.get_local_ip()
        print(f"[CLUSTER] IP local detectada: {self.local_ip} para el nodo {node_id}")
        
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
                        print(f"[CLUSTER] Puerto {port} en uso, reintentando en 2 segundos... ({attempt + 1}/{max_retries})")
                        time.sleep(2)
                    else:
                        raise e

            self.server_socket.listen(10)

            print(f"[CLUSTER] Servidor de cluster escuchando en puerto {port}")

            while True:
                try:
                    client_socket, addr = self.server_socket.accept()
                    print(f"[CLUSTER] Conexión entrante de {addr[0]}:{addr[1]}")
                    threading.Thread(target=self.handle_client, args=(client_socket,), daemon=True).start()
                except Exception as e:
                    print(f"[CLUSTER] Error aceptando conexión: {e}")
        except Exception as e:
            print(f"[CLUSTER] Error crítico iniciando servidor: {e}")
            # Intentar puerto alternativo
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
                print(f"[CLUSTER] Error decodificando JSON: {e}")
                print(f"[CLUSTER] Datos recibidos (parcial): {data[:200]}...")
                client_socket.send(json.dumps({
                    'status': 'error', 
                    'message': f'Invalid JSON: {str(e)}'
                }).encode())
                
        except Exception as e:
            print(f"[CLUSTER] Error manejando cliente: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass
