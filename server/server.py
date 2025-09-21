import socket
import threading
import os
import time

BUFFER_SIZE = 65536

def start_ftp_server():
    print('Minimal FTP server skeleton')

if __name__ == '__main__':
    start_ftp_server()

import socket
import threading
import os
import time
import platform
import random
import struct
import string
import json
import hashlib
import hmac

BUFFER_SIZE = 65536
# Límite de intentos fallidos y tiempo de bloqueo
MAX_FAILED_ATTEMPTS = 3
BLOCK_TIME = 300  # 5 minutos
INACTIVITY_TIMEOUT = 180  # 3 minutos

# Ajusta este directorio al lugar donde quieras que residan los datos en el host
SERVER_ROOT = os.environ.get('FTP_ROOT', '/server') # Busca la VARIABLE DE ENTORNO FTP_ROOT, o usa /server como Fallbck

HOST = "0.0.0.0"
PORT = 21

failed_attempts = {}

class Session:
    def __init__(self, client_socket, client_addr):
        self.client_socket = client_socket
        self.client_addr = client_addr
        self.client_ip = client_addr[0]
        self.authenticated = False
        self.username = None
        base_root = os.path.abspath(SERVER_ROOT)
        os.makedirs(base_root, exist_ok=True)
        self.root_dir = base_root
        self.current_dir = self.root_dir
        self.type = 'A'   # ASCII por defecto
        self.mode = 'S'   # Stream por defecto
        self.stru = 'F'   # File por defecto
        self.passive_listener = None
        self.data_socket = None
        self.rename_from = None
        self.last_activity = time.time()

# -------------------- Gestión de usuarios --------------------

# --- Rutas base multiplataforma ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))   # carpeta donde está server.py
SERVER_ROOT = BASE_DIR                                  # raíz real del servidor
USERS_FILE = os.path.normpath(os.path.join(SERVER_ROOT, "users.json"))

def hash_password(password: str) -> str:
    """Devuelve hash PBKDF2 seguro."""
