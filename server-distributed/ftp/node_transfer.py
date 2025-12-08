import socket
import threading
import struct
import hashlib
import os
import time
import json
from typing import Tuple
from ftp.paths import calculate_file_hash, SERVER_ROOT


class NodeTransfer:
    NODE_DATA_PORT = 2125  # Puerto para transferencias entre nodos
    BUFFER_SIZE = 65536    # Tamaño del buffer para transferencias
    
    def __init__(self, local_ip: str):
        self.local_ip = local_ip
        self.transfer_socket = None
        self.active_transfers = {}
        self.transfer_lock = threading.Lock()
        
    def start_server(self):
        """Inicia servidor para recibir transferencias de otros nodos."""
        try:
            self.transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.transfer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.transfer_socket.bind(('0.0.0.0', self.NODE_DATA_PORT))
            self.transfer_socket.listen(10)
            
            print(f"[NODE-TRANSFER] Servidor escuchando en puerto {self.NODE_DATA_PORT}")
            
            while True:
                try:
                    client_sock, addr = self.transfer_socket.accept()
                    threading.Thread(
                        target=self.handle_node_connection,
                        args=(client_sock, addr),
                        daemon=True
                    ).start()
                except Exception as e:
                    print(f"[NODE-TRANSFER] Error aceptando conexión: {e}")

        except Exception as e:
            print(f"[NODE-TRANSFER] Error crítico iniciando servidor: {e}")
            # Intentar puerto alternativo
            if self.NODE_DATA_PORT == 2125:
                print("[NODE-TRANSFER] Intentando puerto alternativo 2126...")
                self.NODE_DATA_PORT = 2126
                self.start_server()
    
    # --- MANEJO DE CONEXIONES ---

    def handle_node_connection(self, client_sock, addr):
        """Maneja conexión entrante de otro nodo."""
        try:
            # Leer tipo de mensaje (1 byte)
            msg_type_data = client_sock.recv(1)
            if not msg_type_data:
                return
            
            msg_type = struct.unpack("B", msg_type_data)[0]
            
            if msg_type == 1:  # Solicitud de archivo (PULL)
                self.send_file_to_node(client_sock, addr)
            elif msg_type == 2:  # Envío de archivo (PUSH)
                self.receive_file_from_node(client_sock, addr)
            elif msg_type == 3:  # Comando de sincronización
                self.handle_sync_command(client_sock, addr)
            elif msg_type == 4:  # Ping/health check
                client_sock.send(b"PONG")
            else:
                print(f"[NODE-TRANSFER] Tipo de mensaje desconocido: {msg_type}")
                
        except Exception as e:
            print(f"[NODE-TRANSFER] Error en conexión con {addr}: {e}")
        finally:
            try:
                client_sock.close()
            except:
                pass
        
    def send_file_to_node(self, sock, addr):
        """Envía un archivo a otro nodo (Respuesta a PULL)."""
        try:
            # Leer longitud del nombre de archivo (4 bytes)
            filename_len_data = sock.recv(4)
            if len(filename_len_data) < 4:
                return
            
            filename_len = struct.unpack(">I", filename_len_data)[0]
            
            # Leer nombre de archivo solicitado
            filename_data = sock.recv(filename_len)
            filename = filename_data.decode()
            
            # --- Buscar archivo en múltiples ubicaciones ---
            file_found = False
            file_to_send = filename
            
            # Intento 1: Ruta absoluta directa
            if os.path.exists(filename):
                file_found = True
                file_to_send = filename
            else:
                # Intento 2: Extraer usuario de la ruta y buscar en su directorio
                parts = filename.split('/')
                
                # Buscar el patrón '/root/' en la ruta
                if 'root' in parts:
                    root_index = parts.index('root')
                    if root_index + 1 < len(parts):
                        user = parts[root_index + 1]
                        
                        # Construir ruta base del usuario
                        user_root = os.path.join(SERVER_ROOT, 'root', user)
                        
                        # Construir ruta relativa dentro del directorio del usuario
                        rel_parts = parts[root_index + 2:]  # Todo después de 'root/<usuario>'
                        
                        if rel_parts:
                            # Buscar recursivamente desde el directorio del usuario
                            target_filename = rel_parts[-1]  # Nombre del archivo
                            
                            for root_dir, dirs, files in os.walk(user_root):
                                if target_filename in files:
                                    # Verificar si la ruta relativa coincide
                                    rel_path_from_user = os.path.relpath(root_dir, user_root)
                                    if rel_path_from_user == '.':
                                        # El archivo está directamente en el directorio del usuario
                                        if target_filename == rel_parts[0]:
                                            file_to_send = os.path.join(root_dir, target_filename)
                                            file_found = True
                                            break
                                    else:
                                        # Verificar si la estructura de directorios coincide
                                        expected_rel_dirs = '/'.join(rel_parts[:-1])
                                        if expected_rel_dirs == rel_path_from_user.replace(os.sep, '/'):
                                            file_to_send = os.path.join(root_dir, target_filename)
                                            file_found = True
                                            break
            
            if not file_found:
                # Último intento: buscar por nombre de archivo en todo el árbol
                target_filename = os.path.basename(filename)
                for root_dir, dirs, files in os.walk(SERVER_ROOT):
                    if target_filename in files:
                        file_to_send = os.path.join(root_dir, target_filename)
                        file_found = True
                        break
            
            if not file_found:
                print(f"[NODE-TRANSFER] Archivo no encontrado: {filename}")
                sock.send(struct.pack(">I", 0))  # Enviar error (0)
                return
            
            # Enviar confirmación (1)
            sock.send(struct.pack(">I", 1))  # OK
            encoded_filename = filename.encode()
            sock.send(struct.pack(">I", len(encoded_filename)))
            sock.send(encoded_filename)

            # Obtener información del archivo
            file_size = os.path.getsize(file_to_send)
            file_hash = calculate_file_hash(file_to_send)
            
            # Enviar tamaño (8 bytes)
            sock.send(struct.pack(">Q", file_size))
            
            # Enviar hash (32 bytes para MD5)
            sock.send(file_hash.encode())
            
            # Enviar contenido del archivo
            sent_bytes = 0
            with open(file_to_send, 'rb') as f:
                while True:
                    chunk = f.read(self.BUFFER_SIZE)
                    if not chunk:
                        break
                    sock.sendall(chunk)
                    sent_bytes += len(chunk)
            
            print(f"[NODE-TRANSFER] Archivo {file_to_send} ({sent_bytes} bytes) enviado a {addr}")
            
        except Exception as e:
            print(f"[NODE-TRANSFER] Error enviando archivo a {addr}: {e}")
    
    def receive_file_from_node(self, sock, addr, target_path: str = None) -> Tuple[bool, str, str]:
        """Recibe un archivo de otro nodo."""
        try:
            # Leer longitud del nombre (4 bytes)
            filename_len_data = sock.recv(4)
            if len(filename_len_data) < 4:
                return False, "", "Connection closed (reading filename len)"
            
            filename_len = struct.unpack(">I", filename_len_data)[0]
            
            # Nombre del archivo
            filename_data = sock.recv(filename_len)
            if len(filename_data) < filename_len:
                return False, "", "Connection closed (reading filename)"
            filename = filename_data.decode()
            
            # Si target_path es None, usar el nombre recibido
            save_path = target_path if target_path else filename
            
            # Leer tamaño (8 bytes)
            file_size_data = b""
            while len(file_size_data) < 8:
                chunk = sock.recv(8 - len(file_size_data))
                if not chunk: break
                file_size_data += chunk
            
            if len(file_size_data) < 8:
                return False, save_path, "Connection closed (reading size)"
                
            file_size = struct.unpack(">Q", file_size_data)[0]
            
            # Leer Hash (32 bytes)
            expected_hash_data = b""
            while len(expected_hash_data) < 32:
                chunk = sock.recv(32 - len(expected_hash_data))
                if not chunk: break
                expected_hash_data += chunk
                
            if len(expected_hash_data) < 32:
                return False, save_path, "Connection closed (reading hash)"
                
            expected_hash = expected_hash_data.decode()
            
            print(f"[NODE-TRANSFER] Recibiendo: {save_path}, Size: {file_size}")
            
            # Crear directorios
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # Recibir contenido
            received_size = 0
            hash_obj = hashlib.md5()
            
            with open(save_path, 'wb') as f:
                while received_size < file_size:
                    remaining = file_size - received_size
                    chunk_size = min(self.BUFFER_SIZE, remaining)
                    chunk = sock.recv(chunk_size)
                    if not chunk: break
                    
                    f.write(chunk)
                    hash_obj.update(chunk)
                    received_size += len(chunk)
            
            if received_size != file_size:
                return False, save_path, f"Size mismatch: {received_size} vs {file_size}"
            
            actual_hash = hash_obj.hexdigest()
            if actual_hash != expected_hash:
                return False, save_path, f"Hash mismatch: {actual_hash} vs {expected_hash}"
            
            # Enviar confirmación simple si es posible
            try:
                sock.send(b"OK")
            except: pass
            
            return True, save_path, actual_hash
            
        except Exception as e:
            print(f"[NODE-TRANSFER] Error receiving: {e}")
            import traceback
            traceback.print_exc()
            return False, target_path, str(e)
    
    def handle_sync_command(self, sock, addr):
        """Maneja comandos de sincronización."""
        try:
            # Leer comando (4 bytes de longitud)
            cmd_len_data = sock.recv(4)
            if len(cmd_len_data) < 4:
                return
            
            cmd_len = struct.unpack(">I", cmd_len_data)[0]
            
            # Leer comando JSON
            cmd_data = sock.recv(cmd_len)
            if len(cmd_data) < cmd_len:
                return
            
            command = json.loads(cmd_data.decode())
            cmd_type = command.get('command')
            
            print(f"[NODE-TRANSFER] Comando de sincronización desde {addr}: {cmd_type}")
            
            if cmd_type == 'CREATE_DIR':
                path = command.get('path')
                os.makedirs(path, exist_ok=True)
                sock.send(b"OK")
                
            elif cmd_type == 'DELETE_DIR':
                path = command.get('path')
                if os.path.exists(path) and os.path.isdir(path):
                    if not os.listdir(path):
                        os.rmdir(path)
                        sock.send(b"OK")
                    else:
                        sock.send(b"DIR_NOT_EMPTY")
                else:
                    sock.send(b"NOT_FOUND")
                    
            elif cmd_type == 'DELETE_FILE':
                path = command.get('path')
                if os.path.exists(path) and os.path.isfile(path):
                    os.remove(path)
                    sock.send(b"OK")
                else:
                    sock.send(b"NOT_FOUND")
                    
            elif cmd_type == 'RENAME':
                old_path = command.get('old_path')
                new_path = command.get('new_path')
                if os.path.exists(old_path):
                    os.rename(old_path, new_path)
                    sock.send(b"OK")
                else:
                    sock.send(b"NOT_FOUND")
                    
            else:
                sock.send(b"UNKNOWN_COMMAND")
                
        except Exception as e:
            print(f"[NODE-TRANSFER] Error manejando comando de sincronización: {e}")
            try:
                sock.send(b"ERROR")
            except:
                pass

