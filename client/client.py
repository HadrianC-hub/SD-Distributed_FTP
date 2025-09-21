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


