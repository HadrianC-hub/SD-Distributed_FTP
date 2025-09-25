import socket
import os
import re
import struct

# Variables globales ----------------------------------------------------------------------------------------------------------

BUFFER_SIZE = 65536
TYPE = 'A'
MODE = 'S'
DATA_SOCKET = None                  # Socket de transferencia utilizado para transferencia de datos
DATA_SOCKET_IS_LISTENER = False

# Ejecución principal del cliente ---------------------------------------------------------------------------------------------

# Función que ejecuta los comandos de subida de archivos y carpetas
def cmd_STOR_APPE_STOU(sock, *args, command):
    """Sube un archivo al servidor con STOR / APPE / STOU (modo PASV o PORT)."""
    global DATA_SOCKET, DATA_SOCKET_IS_LISTENER

    # Preparando archivo y destino
    local_path = args[0]
    remote_filename = args[1] if len(args) > 1 else os.path.basename(local_path)
    r_mode = 'r' if TYPE == 'A' else 'rb'

    # Obteniendo socket de datos
    try:
        data_sock = get_socket(sock)
    except Exception as e:
        return False, e

    try:
        # Si estamos en modo PORT: enviar comando y aceptar conexión entrante
        if DATA_SOCKET_IS_LISTENER:
            response = send(sock, f"{command} {remote_filename}")
            if not response.startswith(('1', '2')):
                DATA_SOCKET.close()
                DATA_SOCKET = None
                DATA_SOCKET_IS_LISTENER = False
                return False, response

            DATA_SOCKET.settimeout(5)
            conn, _ = DATA_SOCKET.accept()
            DATA_SOCKET.close()
            DATA_SOCKET = conn
            DATA_SOCKET_IS_LISTENER = False
            data_sock = DATA_SOCKET

        elif data_sock is None:
            return False, "Error: no se pudo establecer canal de datos (PASV)."
        else:
            response = send(sock, f"{command} {remote_filename}")
            if response.startswith('5'):
                data_sock.close()
                DATA_SOCKET = None
                return False, response

        # Transferencia del archivo
        with open(local_path, r_mode) as f:
            if MODE == 'S':  # Stream mode
                for chunk in iter(lambda: f.read(BUFFER_SIZE), b''):
                    send_data_chunk(data_sock, chunk, TYPE)
            else:  # Block mode
                for data in iter(lambda: f.read(BUFFER_SIZE), b''):
                    block_header = struct.pack(">BH", 0x00, len(data))
                    send_data_chunk(data_sock, block_header + data, TYPE)
                try:
                    data_sock.sendall(struct.pack(">BH", 0x80, 0))
                except Exception:
                    pass

    except Exception as e:
        return False, e

    finally:
        if data_sock:
            try:
                data_sock.close()
            except Exception:
                pass
        cleanup_data_socket()

    final_resp = get_response(sock)
    return (True, final_resp) if final_resp.startswith('2') else (False, final_resp)

# Función que ejecuta los comandos USER y PASS para loguearse en el sistema
def connect_to_ftp(host, port, user, password):
    try:
        # Conectar con servidor
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))

        # Leer mensaje de bienvenida
        welcome = sock.recv(1024).decode()

        # Enviar usuario
        sock.sendall(f"USER {user}\r\n".encode())
        resp_user = sock.recv(1024).decode()

        # Enviar contraseña
        sock.sendall(f"PASS {password}\r\n".encode())
        resp_pass = sock.recv(1024).decode()

        return True, sock, f"{welcome}\n{resp_user}\n{resp_pass}"
    except Exception as e:
        return False, None, str(e)

# Función que ejecuta el comando RETR para descargas
def cmd_RETR(sock, remote_path, local_path=None):
    """Recibe un archivo con RETR (soporta modo PASV y PORT)."""
    global DATA_SOCKET, DATA_SOCKET_IS_LISTENER

    try:
        # Resolver ruta local
        if local_path is None:
            os.makedirs("Downloads", exist_ok=True)
            local_path = os.path.join("Downloads", os.path.basename(remote_path))
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)

        w_mode = "w" if TYPE == "A" else "wb"
        data_sock = get_socket(sock)

        # Preparar conexión de datos
        if DATA_SOCKET_IS_LISTENER:
            response = send(sock, f"RETR {remote_path}")
            if not response.startswith(("1", "2")):
                DATA_SOCKET.close()
                DATA_SOCKET = None
                DATA_SOCKET_IS_LISTENER = False
                return False, response

            DATA_SOCKET.settimeout(5)
            conn, _ = DATA_SOCKET.accept()
            DATA_SOCKET.close()
            DATA_SOCKET = conn
            DATA_SOCKET_IS_LISTENER = False
            data_sock = DATA_SOCKET

        elif data_sock is None:
            return False, "Error: no se pudo establecer canal de datos (PASV)."

        else:
            response = send(sock, f"RETR {remote_path}")
            if not response.startswith(("1", "2")):
                data_sock.close()
                DATA_SOCKET = None
                return False, response

        # --- Recepción del archivo ---
        with open(local_path, w_mode) as f:
            if MODE == "S":  # Stream mode
                for chunk in iter(lambda: data_sock.recv(BUFFER_SIZE), b""):
                    write_data_chunk(f, chunk, TYPE)
            else:  # Block mode
                while True:
                    header = data_sock.recv(3)
                    if not header:
                        break
                    try:
                        block_type, block_size = struct.unpack(">BH", header)
                    except Exception:
                        break
                    if block_type == 0x80:
                        break  # EOF
                    remaining = block_size
                    while remaining > 0:
                        chunk = data_sock.recv(min(BUFFER_SIZE, remaining))
                        if not chunk:
                            break
                        write_data_chunk(f, chunk, TYPE)
                        remaining -= len(chunk)

    except Exception as e:
        return False, e

    finally:
        if data_sock:
            try:
                data_sock.close()
            except Exception:
                pass
        cleanup_data_socket()

    # --- Respuesta final ---
    final_resp = get_response(sock)
    return (
        (True, f"Archivo descargado exitosamente: {local_path} - {final_resp}")
        if final_resp.startswith("2")
        else (False, f"Error en transferencia: {final_resp}")
    )


