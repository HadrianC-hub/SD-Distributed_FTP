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
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 260000)
    return f"pbkdf2_sha256$260000${salt.hex()}${dk.hex()}"

def verify_password(stored_hash: str, password: str) -> bool:
    """Verifica contraseña comparando con el hash guardado."""
    try:
        algo, iter_str, salt_hex, hash_hex = stored_hash.split("$")
        salt = bytes.fromhex(salt_hex)
        new_hash = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, int(iter_str))
        return hmac.compare_digest(new_hash.hex(), hash_hex)
    except Exception:
        return False

def load_users():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            return json.load(f)
    return {}

def save_users(users):
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=2)

USERS = load_users()

# -------------------- Utilidades --------------------

def check_failed_attempts(client_ip):
    current_time = time.time()
    if client_ip in failed_attempts:
        info = failed_attempts[client_ip]
        if info['block_time'] > current_time:
            return True
        elif info['block_time'] <= current_time:
            failed_attempts[client_ip] = {'attempts': 0, 'block_time': 0}
    return False

def increment_failed_attempts(client_ip):
    current_time = time.time()
    if client_ip in failed_attempts:
        failed_attempts[client_ip]['attempts'] += 1
    else:
        failed_attempts[client_ip] = {'attempts': 1, 'block_time': 0}
    if failed_attempts[client_ip]['attempts'] >= MAX_FAILED_ATTEMPTS:
        failed_attempts[client_ip]['block_time'] = current_time + BLOCK_TIME
        return True
    return False

def generate_unique_filename(directory, original_filename):
    name, ext = os.path.splitext(original_filename)
    while True:
        unique_suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        unique_name = f"{name}_{unique_suffix}{ext}"
