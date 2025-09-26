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

# Función que recibe la lista de elementos del directorio actual
def cmd_LIST_NLST(sock, *args, command):
    """Envía LIST o NLST y devuelve la respuesta del servidor (soporta PASV y PORT)."""
    global DATA_SOCKET, DATA_SOCKET_IS_LISTENER

    try:
        data_sock = get_socket(sock)
    except Exception as e:
        return f"Error preparando canal de datos: {e}"

    try:
        # --- Preparar comando y canal ---
        cmd_line = f"{command} {args[0]}" if args else command

        if DATA_SOCKET_IS_LISTENER:
            # En modo activo (PORT): enviar comando y aceptar conexión
            response = send(sock, cmd_line)
            if not response.startswith(("1", "2")):
                DATA_SOCKET.close()
                DATA_SOCKET = None
                DATA_SOCKET_IS_LISTENER = False
                return f"Error del servidor: {response}"

            DATA_SOCKET.settimeout(5)
            conn, _ = DATA_SOCKET.accept()
            DATA_SOCKET.close()
            DATA_SOCKET = conn
            DATA_SOCKET_IS_LISTENER = False
            data_sock = DATA_SOCKET

        elif data_sock is None:
            return "Error: no se pudo abrir canal de datos (PASV)."

        else:
            # PASV: conexión ya establecida
            sock.sendall(f"{cmd_line}\r\n".encode())

        # --- Transferencia de datos ---
        prelim = get_response(sock)
        print(prelim.strip())

        data = bytearray()
        for chunk in iter(lambda: data_sock.recv(BUFFER_SIZE), b""):
            data.extend(chunk)

        print(data.decode(errors="ignore"))

        final = get_response(sock)
        return final.strip()

    except Exception as e:
        return f"Error durante LIST/NLST: {e}"

    finally:
        if data_sock:
            try:
                data_sock.close()
            except Exception:
                pass
        cleanup_data_socket()

# Función que ejecuta descargas por un puerto específico
def cmd_PORT(comm_socket, *args):
    """
    Crea un listener local y anuncia el puerto al servidor con PORT.
    Devuelve la respuesta del servidor (string). Si es 2xx, DATA_SOCKET queda como listener
    y DATA_SOCKET_IS_LISTENER = True.
    """
    global DATA_SOCKET, DATA_SOCKET_IS_LISTENER

    args_len = len(args)
    if args_len < 2:
        return "501 Syntax error in parameters or arguments."

    bind_host = args[0]
    try:
        bind_port = int(args[1]) if args[1] else 0
    except Exception:
        return "501 Invalid port number."

    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((bind_host, bind_port))
        listener.listen(1)
        # lo dejamos non-blocking para evitar accept() bloqueante en main thread
        listener.setblocking(False)
    except Exception as e:
        return f"425 Can't create listener: {e}"

    data_ip, data_port = listener.getsockname()
    ip_parts = data_ip.split('.')
    port_high, port_low = divmod(data_port, 256)
    command = f"PORT {','.join(ip_parts)},{port_high},{port_low}"

    response = send(comm_socket, command)
    if response.startswith('2'):
        DATA_SOCKET = listener
        DATA_SOCKET_IS_LISTENER = True
        return response
    else:
        try:
            listener.close()
        except Exception:
            pass
        DATA_SOCKET = None
        DATA_SOCKET_IS_LISTENER = False
        return response

# Función que ejecuta los comandos que no requieren transferencia de datos
def generic_command_by_type(socket, *args, command, command_type):
    """Envía el comando especificado al servidor FTP y recibe una respuesta."""

    args_len = len(args)
    global TYPE, MODE

    if command == 'RNFR':
        if args_len == 1:
            return send(socket, f'RNFR {args[0]}')
        else:
            resp1 = send(socket, f'RNFR {args[0]}')
            if not resp1.startswith('3'):
                return resp1
            resp2 = send(socket, f'RNTO {args[1]}')
            return resp1 + "\n" + resp2

    if command == 'QUIT':
        try:
            return send(socket, 'QUIT')
        finally:
            try:
                socket.close()
            except Exception:
                pass

    if command == 'TYPE':
        response = send(socket, f"TYPE {args[0]}")
        if response.startswith('2'):
            TYPE = args[0].upper()
        return response

    if command == 'MODE':
        response = send(socket, f"MODE {args[0]}")
        if response.startswith('2'):
            MODE = args[0].upper()
        return response

    # Genéricos
    if command_type == 'A':
        return send(socket, f'{command} {args[0]}')
    elif command_type == 'B':
        return send(socket, f'{command}')
    elif command_type == 'C':
        if args_len == 1:
            return send(socket, f'{command} {args[0]}')
        else:
            return send(socket, f'{command}')

    return "Error???"

# Función que limpia los socket de datos
def cleanup_data_socket():
    """
    Cierra cualquier socket de datos o listener y resetea el estado global.
    Llamar siempre al desconectar / reiniciar.
    """
    global DATA_SOCKET, DATA_SOCKET_IS_LISTENER
    try:
        if DATA_SOCKET:
            try:
                DATA_SOCKET.close()
            except Exception:
                pass
    finally:
        DATA_SOCKET = None
        DATA_SOCKET_IS_LISTENER = False

