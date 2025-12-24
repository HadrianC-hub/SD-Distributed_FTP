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
                    print(f"[ERROR][NODE-TRANSFER] Error aceptando conexión: {e}")

        except Exception as e:
            print(f"[ERROR][NODE-TRANSFER] Error crítico iniciando servidor: {e}")
            # Intentar puerto alternativo
            if self.NODE_DATA_PORT == 2125:
                print("[NODE-TRANSFER] Intentando puerto alternativo 2126...")
                self.NODE_DATA_PORT = 2126
                self.start_server()
    
    # --- MANEJO DE CONEXIONES ---

    def handle_node_connection(self, client_sock, addr):
        """Maneja conexión entrante de otro nodo."""
        try:
            msg_type_data = self._recv_exact(client_sock, 1)
            if not msg_type_data:
                return

            msg_type = struct.unpack("B", msg_type_data)[0]

            if msg_type == 1:          # PULL
                self.send_file_to_node(client_sock, addr)
            elif msg_type == 2:        # PUSH
                self.receive_file_from_node(client_sock, addr)
            elif msg_type == 3:        # SYNC
                self.handle_sync_command(client_sock, addr)
            elif msg_type == 4:        # PING
                client_sock.sendall(b"PONG")
            else:
                print(f"[ERROR][NODE-TRANSFER] Tipo de mensaje desconocido: {msg_type}")

        except Exception as e:
            print(f"[ERROR][NODE-TRANSFER] Error en conexión con {addr}: {e}")

        finally:
            try:
                client_sock.close()
            except:
                pass
        
    def send_file_to_node(self, sock, addr):
        """Envía un archivo a otro nodo (Respuesta a PULL)."""
        try:
            filename_len_data = self._recv_exact(sock, 4)
            if len(filename_len_data) < 4:
                return

            filename_len = struct.unpack(">I", filename_len_data)[0]

            filename_data = self._recv_exact(sock, filename_len)
            if len(filename_data) < filename_len:
                return

            filename = filename_data.decode()

            file_found = False
            file_to_send = filename

            # Intento 1: ruta directa
            if os.path.exists(filename):
                file_found = True
                file_to_send = filename
            else:
                parts = filename.split('/')
                if 'root' in parts:
                    root_index = parts.index('root')
                    if root_index + 1 < len(parts):
                        user = parts[root_index + 1]
                        user_root = os.path.join(SERVER_ROOT, 'root', user)
                        rel_parts = parts[root_index + 2:]

                        if rel_parts:
                            target_filename = rel_parts[-1]

                            for root_dir, _, files in os.walk(user_root):
                                if target_filename in files:
                                    rel_path = os.path.relpath(root_dir, user_root)
                                    if rel_path.replace(os.sep, '/') == '/'.join(rel_parts[:-1]) or rel_path == '.':
                                        file_to_send = os.path.join(root_dir, target_filename)
                                        file_found = True
                                        break

            # Último intento global
            if not file_found:
                target_filename = os.path.basename(filename)
                for root_dir, _, files in os.walk(SERVER_ROOT):
                    if target_filename in files:
                        file_to_send = os.path.join(root_dir, target_filename)
                        file_found = True
                        break

            if not file_found:
                print(f"[NODE-TRANSFER] Archivo no encontrado: {filename}")
                sock.sendall(struct.pack(">I", 0))
                return

            sock.sendall(struct.pack(">I", 1))
            encoded_filename = filename.encode()
            sock.sendall(struct.pack(">I", len(encoded_filename)))
            sock.sendall(encoded_filename)

            file_size = os.path.getsize(file_to_send)
            file_hash = calculate_file_hash(file_to_send)

            sock.sendall(struct.pack(">Q", file_size))
            sock.sendall(file_hash.encode())

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
            print(f"[ERROR][NODE-TRANSFER] Error enviando archivo a {addr}: {e}")
    
    def receive_file_from_node(self, sock, addr, target_path: str = None) -> Tuple[bool, str, str]:
        """Recibe un archivo de otro nodo."""
        try:
            filename_len_data = self._recv_exact(sock, 4)
            if len(filename_len_data) < 4:
                return False, "", "Connection closed (reading filename len)"

            filename_len = struct.unpack(">I", filename_len_data)[0]

            filename_data = self._recv_exact(sock, filename_len)
            if len(filename_data) < filename_len:
                return False, "", "Connection closed (reading filename)"

            filename = filename_data.decode()
            save_path = target_path if target_path else filename

            file_size_data = self._recv_exact(sock, 8)
            if len(file_size_data) < 8:
                return False, save_path, "Connection closed (reading size)"

            file_size = struct.unpack(">Q", file_size_data)[0]

            expected_hash_data = self._recv_exact(sock, 32)
            if len(expected_hash_data) < 32:
                return False, save_path, "Connection closed (reading hash)"

            expected_hash = expected_hash_data.decode()

            print(f"[NODE-TRANSFER] Recibiendo: {save_path}, Size: {file_size}")

            os.makedirs(os.path.dirname(save_path), exist_ok=True)

            received_size = 0
            hash_obj = hashlib.md5()

            with open(save_path, 'wb') as f:
                while received_size < file_size:
                    chunk = sock.recv(min(self.BUFFER_SIZE, file_size - received_size))
                    if not chunk:
                        break
                    f.write(chunk)
                    hash_obj.update(chunk)
                    received_size += len(chunk)

            if received_size != file_size:
                return False, save_path, f"Size mismatch: {received_size} vs {file_size}"

            actual_hash = hash_obj.hexdigest()
            if actual_hash != expected_hash:
                return False, save_path, f"Hash mismatch: {actual_hash} vs {expected_hash}"

            try:
                sock.sendall(b"OK")
            except:
                pass

            return True, save_path, actual_hash

        except Exception as e:
            print(f"[ERROR][NODE-TRANSFER] Error receiving: {e}")
            return False, target_path, str(e)
    
    def handle_sync_command(self, sock, addr):
        """Maneja comandos de sincronización."""
        try:
            cmd_len_data = self._recv_exact(sock, 4)
            if len(cmd_len_data) < 4:
                return

            cmd_len = struct.unpack(">I", cmd_len_data)[0]
            cmd_data = self._recv_exact(sock, cmd_len)
            if len(cmd_data) < cmd_len:
                return

            command = json.loads(cmd_data.decode())
            cmd_type = command.get('command')

            print(f"[NODE-TRANSFER] Comando de sincronización desde {addr}: {cmd_type}")

            if cmd_type == 'CREATE_DIR':
                os.makedirs(command.get('path'), exist_ok=True)
                sock.sendall(b"OK")

            elif cmd_type == 'DELETE_DIR':
                path = command.get('path')
                if os.path.exists(path) and os.path.isdir(path):
                    if not os.listdir(path):
                        os.rmdir(path)
                        sock.sendall(b"OK")
                    else:
                        sock.sendall(b"DIR_NOT_EMPTY")
                else:
                    sock.sendall(b"NOT_FOUND")

            elif cmd_type == 'DELETE_FILE':
                path = command.get('path')
                if os.path.exists(path) and os.path.isfile(path):
                    os.remove(path)
                    sock.sendall(b"OK")
                else:
                    sock.sendall(b"NOT_FOUND")

            elif cmd_type == 'RENAME':
                old_path = command.get('old_path')
                new_path = command.get('new_path')
                if os.path.exists(old_path):
                    os.rename(old_path, new_path)
                    sock.sendall(b"OK")
                else:
                    sock.sendall(b"NOT_FOUND")

            else:
                sock.sendall(b"UNKNOWN_COMMAND")

        except Exception as e:
            print(f"[ERROR][NODE-TRANSFER] Error manejando comando de sincronización: {e}")
            try:
                sock.sendall(b"ERROR")
            except:
                pass

    # --- FUNCIONES AUXILIARES ---
                
    def request_file_from_node(self, target_ip: str, file_path: str, local_path: str = None) -> Tuple[bool, str, str]:
        """Solicita un archivo a otro nodo con reintentos."""
        max_retries = 3
        if local_path is None:
            local_path = file_path

        for attempt in range(max_retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(15)
                    sock.connect((target_ip, self.NODE_DATA_PORT))

                    sock.sendall(struct.pack("B", 1))
                    encoded_path = file_path.encode()
                    sock.sendall(struct.pack(">I", len(encoded_path)))
                    sock.sendall(encoded_path)

                    response_data = self._recv_exact(sock, 4)
                    if len(response_data) < 4:
                        return False, local_path, "No response from target"

                    response = struct.unpack(">I", response_data)[0]
                    if response == 0:
                        return False, local_path, "File not found on target"

                    success, actual_path, msg = self.receive_file_from_node(sock, target_ip, local_path)
                    if success:
                        return True, actual_path, msg

                    print(f"[NODE-TRANSFER] Intento {attempt + 1} falló para {file_path}: {msg}")

            except Exception as e:
                print(f"[NODE-TRANSFER] Error en intento {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    return False, local_path, str(e)

            if attempt < max_retries - 1:
                time.sleep(1)

        return False, local_path, f"Failed after {max_retries} attempts"

    def _recv_exact(self, sock, size: int) -> bytes:
        data = b""
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                break
            data += chunk
        return data


# Singleton
_node_transfer_instance = None
def get_node_transfer(local_ip: str = None):
    global _node_transfer_instance
    if _node_transfer_instance is None:
        if not local_ip: local_ip = '127.0.0.1'
        _node_transfer_instance = NodeTransfer(local_ip)
        threading.Thread(target=_node_transfer_instance.start_server, daemon=True).start()
    return _node_transfer_instance
