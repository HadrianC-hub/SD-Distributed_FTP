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
        if not os.path.exists(os.path.join(directory, unique_name)):
            return unique_name

def safe_path(session, path):
    """
    Devuelve la ruta normalizada dentro del root del session.
    Lanza PermissionError si la ruta sale del root.
    """
    if os.path.isabs(path):
        candidate = os.path.normpath(path)
    else:
        candidate = os.path.normpath(os.path.join(session.current_dir, path))
    # Obtener rutas absolutas
    candidate_abs = os.path.abspath(candidate)
    root_abs = os.path.abspath(session.root_dir)
    if not candidate_abs.startswith(root_abs):
        raise PermissionError("Access outside of user root")
    return candidate_abs
            
def get_advertised_ip_for_session(comm_socket):
    # 1) env var (si existe y no es loopback)
    env_ip = os.environ.get('FTP_PASV_ADDRESS', '').strip()
    if env_ip:
        try:
            resolved = socket.gethostbyname(env_ip)
            if not resolved.startswith('127.'):
                return resolved
        except Exception:
            pass

    # 2) IP local del socket de control (lo más fiable en Docker compose)
    try:
        local_ip = comm_socket.getsockname()[0]
        if local_ip and not local_ip.startswith('127.') and local_ip != '0.0.0.0':
            return local_ip
    except Exception:
        pass

    # 3) "UDP trick": averiguar la IP usada para salir a Internet (no realiza conexión real)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        if ip and not ip.startswith('127.'):
            return ip
    except Exception:
        pass

    # 4) fallback: host resolution (menos fiable)
    try:
        host_ip = socket.gethostbyname(socket.gethostname())
        if not host_ip.startswith('127.'):
            return host_ip
    except Exception:
        pass

    return '127.0.0.1'

# -------------------- Comandos básicos --------------------

def cmd_USER(arg, session):
    if not arg:
        session.client_socket.send(b"501 Syntax error in parameters or arguments.\r\n")
        return None
    if arg.lower() == "anonymous":
        session.username = "anonymous"
        session.authenticated = True
        session.client_socket.send(b"230 Anonymous access granted, restrictions may apply.\r\n")
        return session.username
    if arg in USERS:
        session.username = arg
        session.client_socket.send(b"331 User name okay, need password.\r\n")
        return session.username
    else:
        session.client_socket.send(b"530 User not found.\r\n")
        if increment_failed_attempts(session.client_ip):
            session.client_socket.send(b"421 Too many failed login attempts. Try again later.\r\n")
        session.username = None
        return None

def cmd_PASS(arg, session):
    if session.username is None:
        session.client_socket.send(b"503 Bad sequence of commands.\r\n")
        return False
    if session.authenticated:
        session.client_socket.send(b"202 Already logged in.\r\n")
        return True

    user_info = USERS.get(session.username)
    if user_info and verify_password(user_info["password"], arg):
        user_root = os.path.join(SERVER_ROOT, 'root', session.username)
        os.makedirs(user_root, exist_ok=True)
        session.root_dir = session.current_dir = user_root
        session.authenticated = True
        session.client_socket.send(b"230 Login successful.\r\n")
        return True
    else:
        session.client_socket.send(b"530 Login incorrect.\r\n")
        increment_failed_attempts(session.client_ip)
        return False

def cmd_ACCT(arg, session):
    if session.authenticated:
        session.client_socket.send(b"202 No additional account information required.\r\n")
    elif session.username is None:
        session.client_socket.send(b"503 Bad sequence of commands.\r\n")
    elif not arg:
