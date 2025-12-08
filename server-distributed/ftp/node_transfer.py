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
