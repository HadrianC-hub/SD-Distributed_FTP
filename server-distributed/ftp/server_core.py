import socket
import os
import threading
import time
import ftp.commands as command
from ftp.commands import SERVER_ROOT, BUFFER_SIZE

# Tiempo de inactividad hasta el cierre de la cuenta
INACTIVITY_TIMEOUT = 180  # 3 minutos

# Dirección de escucha del servidor (escuchar en todas las direcciones)
HOST = "0.0.0.0"
PORT = 21

class Session: # Clase para manejar sesiones del usuario
    def __init__(self, client_socket, client_addr):
        self.client_socket = client_socket
        self.client_addr = client_addr
        self.client_ip = client_addr[0]
        self.authenticated = False
        self.username = None
        base_root = os.path.abspath(SERVER_ROOT)
        os.makedirs(base_root, exist_ok=True)
        root_dir = os.path.join(base_root, 'root')
        os.makedirs(root_dir, exist_ok=True)
        self.root_dir = root_dir
        self.current_dir = self.root_dir
        self.type = 'A'   # ASCII por defecto
        self.mode = 'S'   # Stream por defecto
        self.stru = 'F'   # File por defecto
        self.passive_listener = None
        self.data_socket = None
        self.rename_from = None
        self.last_activity = time.time()

# --- PARSING Y CONTROL ---

def handle_command_line(line, session):
    """
    Devuelve True si se debe terminar la conexión (QUIT), False en otro caso.
    Actualiza session segun comandos.
    """
    line = line.rstrip('\r\n')
    if not line:
        return False
    parts = line.split(' ', 1)
    cmd = parts[0].upper()
    arg = parts[1] if len(parts) > 1 else None

    if cmd == "USER":
        command.USER(arg, session)
    elif cmd == "PASS":
        command.PASS(arg, session)
    elif cmd == "ACCT":
        command.ACCT(arg, session)
    elif cmd == "SMNT":
        command.SMNT(arg, session)
    elif cmd == "REIN":
        command.REIN(session)
    elif cmd == "QUIT":
        session.client_socket.send(b"221 Goodbye.\r\n")
        return True
    elif cmd == "PWD":
        command.PWD(session)
    elif cmd == "CWD":
        command.CWD(arg, session)
    elif cmd == "CDUP":
        command.CDUP(session)
    elif cmd == "MKD":
        command.MKD(arg, session)
    elif cmd == "RMD":
        command.RMD(arg, session)
    elif cmd == "DELE":
        command.DELE(arg, session)
    elif cmd == "TYPE":
        command.TYPE(arg, session)
    elif cmd == "MODE":
        command.MODE(arg, session)
    elif cmd == "SYST":
        command.SYST(session)
    elif cmd == "STAT":
        command.STAT(arg, session)
    elif cmd == "HELP":
        command.HELP(arg, session)
    elif cmd == "RNFR":
        command.RNFR(arg, session)
    elif cmd == "RNTO":
        command.RNTO(arg, session)
    elif cmd == "NOOP":
        command.NOOP(session)
    elif cmd == "PASV":
        command.PASV(session)
    elif cmd == "RETR":
        command.RETR(arg, session)
    elif cmd == "STOR":
        command.STOR(arg, session, append=False, unique=False)
    elif cmd == "APPE":
        command.APPE(arg, session)
    elif cmd == "STOU":
        command.STOU(arg, session)
    elif cmd == "LIST":
        command.LIST(session)
    elif cmd == "NLST":
        command.NLST(session)
    elif cmd == "ABOR":
        command.ABOR(session)
    elif cmd == "PORT":
        command.PORT(arg, session)
    else:
        session.client_socket.send(b"502 Command not implemented.\r\n")
    return False

def handle_client(client_socket, address):
    session = Session(client_socket, address)
    if check_failed_attempts(session.client_ip):
        session.client_socket.send(b"421 Too many failed login attempts. Try again later.\r\n")
        client_socket.close()
        return

    print(f"[CORE] Conexión establecida desde {address}")
    client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    session.client_socket.send(b"220 FTP Server Ready\r\n")

    while True:
        try:
            # Aplicar timeout por actividad
            session.client_socket.settimeout(1.0)
            try:
                data = session.client_socket.recv(BUFFER_SIZE)
            except socket.timeout:
                # Verificar inactividad
                if time.time() - session.last_activity > INACTIVITY_TIMEOUT:
                    session.client_socket.send(b"421 Service timeout.\r\n")
                    break
                continue
            if not data:
                break
            try:
                text = data.decode()
            except Exception:
                text = data.decode(errors='ignore')
            session.last_activity = time.time()
            print(f"[CORE] Comando recibido de {address}: {text.strip()}")
            should_quit = handle_command_line(text, session)
            if should_quit:
                break
        except ConnectionResetError:
            break
        except Exception as e:
            # No queremos que un cliente nos tumbe todo el server
            print(f"[ERROR][CORE] Error en client handler {address}: {e}")
            break

    print(f"[CORE] Conexión cerrada con {address}")
    try:
        if session.passive_listener:
            session.passive_listener.close()
    except Exception:
        pass
    command.close_data_socket(session)
    try:
        session.client_socket.close()
    except Exception:
        pass

def start_ftp_server(listen_host=HOST, listen_port=PORT):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((listen_host, listen_port))
    server_socket.listen(5)
    print(f"Servidor FTP escuchando en {listen_host}:{listen_port}")
    try:
        while True:
            client_socket, address = server_socket.accept()
            t = threading.Thread(target=handle_client, args=(client_socket, address), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("Servidor detenido por teclado")
    finally:
        server_socket.close()

# --- UTILIDADES ---

def check_failed_attempts(client_ip):
    current_time = time.time()
    if client_ip in command.failed_attempts:
        info = command.failed_attempts[client_ip]
        if info['block_time'] > current_time:
            return True
        elif info['block_time'] <= current_time:
            command.failed_attempts[client_ip] = {'attempts': 0, 'block_time': 0}
    return False
