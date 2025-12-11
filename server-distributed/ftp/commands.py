import socket
import os
import time
import random
import threading
from typing import Dict

failed_attempts = {}        # Diccionario de intentos fallidos de login por IP
MAX_FAILED_ATTEMPTS = 3     # Límite de intentos fallidos y tiempo de bloqueo
BLOCK_TIME = 300            # Tiempo de bloqueo de IP
BUFFER_SIZE = 65536         # Tamaño máximo del buffer

# Variables globales añadidas
active_operations = {}  # operation_id -> información de operación
operation_lock = threading.Lock()
active_delta_transfers = {}
delta_lock = threading.Lock()

# --- COMANDOS BASICOS ---

def USER(arg, session):
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

def PASS(arg, session):
    if session.username is None:
        session.client_socket.send(b"503 Bad sequence of commands.\r\n")
        return False
    if session.authenticated:
        session.client_socket.send(b"202 Already logged in.\r\n")
        return True

    user_info = USERS.get(session.username)
    if user_info and verify_password(user_info["password"], arg):
        user_root = os.path.join(SERVER_ROOT, 'root', session.username)
        # Crear directorio localmente
        os.makedirs(user_root, exist_ok=True)
        session.root_dir = session.current_dir = user_root
        session.authenticated = True
        
        # Notificar al líder sobre el directorio de usuario
        cluster_comm = get_global_cluster_comm()
        if cluster_comm:
            # Solo si no somos el líder
            bully = get_global_bully()
            if bully and not bully.am_i_leader():
                response = ask_leader('ENSURE_USER_DIR', {
                    'user_root': user_root,
                    'session_user': session.username
                })
                print(f"[AUTH] Directorio de usuario notificado al líder: {response.get('status')}")
            else:
                # Si soy líder, crear en mi estado global
                from ftp.state_manager import get_state_manager
                state_mgr = get_state_manager()
                state_mgr.ensure_user_directory(user_root)
        
        session.client_socket.send(b"230 Login successful.\r\n")
        return True
    else:
        session.client_socket.send(b"530 Login incorrect.\r\n")
        increment_failed_attempts(session.client_ip)
        return False

def ACCT(arg, session):
    session.client_socket.send(b"202 Command ignored.\r\n")

def SMNT(arg, session):
    session.client_socket.send(b"202 Command ignored.\r\n")

def REIN(session):
    session.username = None
    session.authenticated = False
    session.current_dir = os.path.abspath(SERVER_ROOT)
    session.client_socket.send(b"220 Service ready for new user.\r\n")

def PWD(session):
    """Consulta al líder para obtener la ruta lógica actual."""
    if not session.authenticated:
        session.client_socket.send(b"530 Not logged in.\r\n")
        return
    
    # Consultar al líder
    cluster_comm = get_global_cluster_comm()
    if not cluster_comm:
        session.client_socket.send(b"550 Cluster not available.\r\n")
        return
    
    response = ask_leader('PWD', {
        'abs_path': session.current_dir,
        'user_root': session.root_dir,
        'session_user': session.username
    })
    
    if response.get('status') != 'ok':
        error_msg = response.get('message', 'Error')
        session.client_socket.send(f"550 {error_msg}\r\n".encode())
        return
    
    # Convertir la ruta absoluta devuelta por el líder a relativa
    abs_path = response.get('abs_path', session.current_dir)
    rel_path = os.path.relpath(abs_path, session.root_dir)
    if rel_path == '.':
        rel_path = '/'
    else:
        rel_path = '/' + rel_path.replace(os.sep, '/')
    
    session.client_socket.send(f'257 "{rel_path}" is the current directory.\r\n'.encode())

def CWD(arg, session):
    """Cambia el directorio consultando al líder."""
    if not session.authenticated:
        session.client_socket.send(b"530 Not logged in.\r\n")
        return
    if not arg:
        session.client_socket.send(b"501 Syntax error in parameters or arguments.\r\n")
        return
    
    try:
        new_path = safe_path(session, arg)
    except PermissionError:
        session.client_socket.send(b"550 Access denied.\r\n")
        return
    
    # Consultar al líder
    cluster_comm = get_global_cluster_comm()
    if not cluster_comm:
        session.client_socket.send(b"550 Cluster not available.\r\n")
        return
    
    response = ask_leader('CWD', {
        'abs_path': new_path,
        'user_root': session.root_dir,
        'session_user': session.username
    })
    
    if response.get('status') != 'ok':
        error_msg = response.get('message', 'Error')
        session.client_socket.send(f"550 {error_msg}\r\n".encode())
        return
    
    # Actualizar directorio actual
    session.current_dir = new_path
    session.client_socket.send(b"250 Directory successfully changed.\r\n")

def CDUP(session):
    """Cambia al directorio padre consultando al líder."""
    if not session.authenticated:
        session.client_socket.send(b"530 Not logged in.\r\n")
        return
    
    # Calcular directorio padre
    parent = os.path.abspath(os.path.join(session.current_dir, '..'))
    root_abs = os.path.abspath(session.root_dir)
    
    # Verificar que no salimos del root del usuario
    if not parent.startswith(root_abs):
        parent = root_abs  # Si intentamos salir del root, permanecemos en él
    
    # Consultar al líder
    cluster_comm = get_global_cluster_comm()
    if not cluster_comm:
        session.client_socket.send(b"550 Cluster not available.\r\n")
        return
    
    response = ask_leader('CDUP', {
        'abs_path': session.current_dir,
        'user_root': session.root_dir,
        'session_user': session.username
    })
    
    if response.get('status') != 'ok':
        error_msg = response.get('message', 'Error')
        session.client_socket.send(f"550 {error_msg}\r\n".encode())
        return
    
    # Actualizar directorio actual con la respuesta del líder
    new_dir = response.get('abs_path', parent)
    session.current_dir = new_dir
    session.client_socket.send(b"250 Directory successfully changed.\r\n")

def MKD(arg, session):
    """Crea un directorio."""
    if not session.authenticated:
        session.client_socket.send(b"530 Not logged in.\r\n")
        return
    
    if not arg:
        session.client_socket.send(b"501 Syntax error.\r\n")
        return
    
    # Consultar al líder
    cluster_comm = get_global_cluster_comm()
    if not cluster_comm:
        session.client_socket.send(b"550 Cluster not available.\r\n")
        return
    
    # Obtener ruta completa
    try:
        target_path = safe_path(session, arg)
    except PermissionError:
        session.client_socket.send(b"550 Access denied.\r\n")
        return
    
    response = ask_leader('MKD', {
        'path': target_path,
        'requester': cluster_comm.local_ip,
        'session_user': session.username
    })
    
    if response.get('status') != 'ok':
        error_msg = response.get('message', 'Error')
        error_code = response.get('code', '550')
        session.client_socket.send(f"{error_code} {error_msg}\r\n".encode())
        return
    
    # Ejecutar localmente y en otros nodos
    operation_id = response.get('operation_id')
    nodes = response.get('nodes', [])
    command = response.get('command')
    
    if command == 'CREATE_DIR':
        # Crear localmente
        try:
            os.makedirs(target_path, exist_ok=True)
            print(f"[MKD] Directorio creado localmente: {target_path}")
            
            # Si soy el líder, ordenar a otros nodos
            bully = get_global_bully()
            if bully and bully.am_i_leader():
                order_operation_to_nodes('CREATE_DIR', target_path, nodes, operation_id)
            
            session.client_socket.send(f'257 "{arg}" directory created.\r\n'.encode())
            
            # Registrar operación local
            register_local_operation(operation_id, 'MKD', target_path, session.username)
            
        except Exception as e:
            print(f"[MKD] Error creating directory: {e}")
            session.client_socket.send(b"550 Failed to create directory.\r\n")
    else:
        session.client_socket.send(b"550 Unexpected response from leader.\r\n")

def RMD(arg, session):
    """Elimina un directorio en TODOS los nodos."""
    if not session.authenticated:
        session.client_socket.send(b"530 Not logged in.\r\n")
        return
    if not arg:
        session.client_socket.send(b"501 Syntax error.\r\n")
        return
    
    # Consultar al líder
    cluster_comm = get_global_cluster_comm()
    if not cluster_comm:
        session.client_socket.send(b"550 Cluster not available.\r\n")
        return
    
    # Obtener ruta completa
    try:
        target_path = safe_path(session, arg)
    except PermissionError:
        session.client_socket.send(b"550 Access denied.\r\n")
        return
    
    # NO ejecutar localmente aquí - el líder coordinará el borrado en todos los nodos
    response = ask_leader('RMD', {
        'path': target_path,
        'requester': cluster_comm.local_ip,
        'session_user': session.username,
        'delete_all': True  # Indicar borrado completo
    })
    
    if response.get('status') != 'ok':
        error_msg = response.get('message', 'Error')
        error_code = response.get('code', '550')
        session.client_socket.send(f"{error_code} {error_msg}\r\n".encode())
        return
    
    # El líder ha coordinado el borrado en todos los nodos
    session.client_socket.send(f'250 "{arg}" directory removed from all nodes.\r\n'.encode())

def DELE(arg, session):
    """Elimina un archivo en TODOS los nodos que lo contengan."""
    if not session.authenticated:
        session.client_socket.send(b"530 Not logged in.\r\n")
        return
    if not arg:
        session.client_socket.send(b"501 Syntax error in parameters or arguments.\r\n")
        return
    
    # Consultar al líder
    cluster_comm = get_global_cluster_comm()
    if not cluster_comm:
        session.client_socket.send(b"550 Cluster not available.\r\n")
        return
    
    # Obtener ruta completa
    try:
        target_path = safe_path(session, arg)
    except PermissionError:
        session.client_socket.send(b"550 Access denied.\r\n")
        return
    
    # Enviar solicitud al líder
    response = ask_leader('DELE', {
        'path': target_path,
        'requester': cluster_comm.local_ip,
        'session_user': session.username
    })
    
    if response.get('status') != 'ok':
        error_msg = response.get('message', 'Error')
        error_code = response.get('code', '550')
        session.client_socket.send(f"{error_code} {error_msg}\r\n".encode())
        return
    
    # El líder ha coordinado la eliminación en todos los nodos
    session.client_socket.send(f"250 Deleted {arg}.\r\n".encode())

def TYPE(arg, session):
    if not arg:
        session.client_socket.send(b"501 Syntax error.\r\n")
        return
    a = arg.upper()
    if a == 'A':
        session.type = 'A'
        session.client_socket.send(b"200 Type set to ASCII.\r\n")
    elif a == 'I':
        session.type = 'I'
        session.client_socket.send(b"200 Type set to Binary.\r\n")
    else:
        session.client_socket.send(b"501 Syntax error.\r\n")

def MODE(arg, session):
    if not arg:
        session.client_socket.send(b"501 Syntax error in parameters or arguments.\r\n")
        return
    a = arg.upper()
    if a in ('S','B','C'):
        session.mode = a
        session.client_socket.send(b"200 Mode set.\r\n")
    else:
        session.client_socket.send(b"501 Syntax error in parameters or arguments.\r\n")

def SYST(session):
    session.client_socket.send(b"215 UNIX Type: L8\r\n")

def STAT(arg, session):
    session.client_socket.send(b"211 Status OK.\r\n")

def HELP(arg, session):
    # Mensajes simplificados
    help_msg = "214-The following commands are recognized:\r\n" \
               "USER PASS CWD CDUP QUIT REIN PORT PASV TYPE MODE" \
               "RETR STOR APPE STOU LIST NLST STAT NOOP HELP PWD MKD RMD DELE RNFR RNTO\r\n" \
               "214 End of help message.\r\n"
    session.client_socket.send(help_msg.encode())

def RNFR(arg, session):
    """Solicita renombrar un archivo/directorio al líder."""
    if not session.authenticated:
        session.client_socket.send(b"530 Not logged in.\r\n")
        return
    
    if not arg:
        session.client_socket.send(b"550 No file specified.\r\n")
        return
    
    try:
        # Obtener ruta completa (para que el líder entienda la ruta lógica)
        target_path = safe_path(session, arg)
    except PermissionError:
        session.client_socket.send(b"550 Access denied.\r\n")
        return
    
    # Consultar al líder para obtener lock
    cluster_comm = get_global_cluster_comm()
    if not cluster_comm:
        session.client_socket.send(b"550 Cluster not available.\r\n")
        return
    
    # Enviar solicitud al líder - NO VERIFICAR LOCALMENTE
    response = ask_leader('RNFR', {
        'path': target_path,
        'requester': cluster_comm.local_ip,
        'session_user': session.username
    })
    
    if response.get('status') != 'ok':
        error_msg = response.get('message', 'Error')
        session.client_socket.send(f"550 {error_msg}\r\n".encode())
        return
    
    # Guardar información para RNTO
    session.rename_from = target_path
    session.rename_lock_id = response.get('lock_id')
    session.rename_type = response.get('type', 'file')  # Guardar tipo (file/dir)
    print(f"[RNFR] Lock adquirido: {session.rename_lock_id} para {target_path}")
    
    session.client_socket.send(b"350 Ready for RNTO.\r\n")

def RNTO(arg, session):
    """Ejecuta el renombrado coordinado por el líder."""
    if not session.authenticated:
        session.client_socket.send(b"530 Not logged in.\r\n")
        return
    
    if not session.rename_from or not session.rename_lock_id:
        session.client_socket.send(b"503 Bad sequence of commands.\r\n")
        return
    
    if not arg:
        session.client_socket.send(b"501 Syntax error in parameters or arguments.\r\n")
        return
    
    # Validar el nuevo nombre
    is_valid, error_msg = is_valid_filename(arg)
    if not is_valid:
        session.client_socket.send(f"553 Requested action not taken. {error_msg}.\r\n".encode())
        session.rename_from = None
        session.rename_lock_id = None
        session.rename_type = None
        return
    
    try:
        # Obtener ruta completa del nuevo nombre
        new_path = safe_path(session, arg)
    except PermissionError:
        session.client_socket.send(b"550 Access denied.\r\n")
        session.rename_from = None
        session.rename_lock_id = None
        session.rename_type = None
        return
    
    # Consultar al líder para ejecutar el renombrado
    cluster_comm = get_global_cluster_comm()
    if not cluster_comm:
        session.client_socket.send(b"550 Cluster not available.\r\n")
        session.rename_from = None
        session.rename_lock_id = None
        session.rename_type = None
        return
    
    # Para directorios, verificar que no haya conflicto con rutas existentes
    if session.rename_type == 'dir':
        # Verificar que el nuevo nombre no sea subdirectorio del antiguo
        if new_path.startswith(session.rename_from.rstrip('/') + '/'):
            session.client_socket.send(b"553 Cannot move directory inside itself.\r\n")
            session.rename_from = None
            session.rename_lock_id = None
            session.rename_type = None
            return
    
    # Enviar solicitud al líder - NO INTENTAR RENOMBRAR LOCALMENTE
    response = ask_leader('RNTO', {
        'old_path': session.rename_from,
        'new_path': new_path,
        'lock_id': session.rename_lock_id,
        'requester': cluster_comm.local_ip,
        'session_user': session.username
    })
    
    if response.get('status') != 'ok':
        error_msg = response.get('message', 'Error')
        session.client_socket.send(f"550 {error_msg}\r\n".encode())
        session.rename_from = None
        session.rename_lock_id = None
        session.rename_type = None
        return
    
    # Limpiar estado de la sesión
    session.rename_from = None
    session.rename_lock_id = None
    session.rename_type = None
    
    session.client_socket.send(b"250 Rename successful.\r\n")

def NOOP(session):
    session.client_socket.send(b"200 NOOP command successful.\r\n")

def PASV(session, data_port_range=(21000, 21100)):
    """
    Abre un listener PASV, lo guarda en session.passive_listener y envía la 227.
    No hace accept() aquí — accept_passive_connection(session) lo hará luego.
    """
    min_p, max_p = data_port_range
    ports = list(range(min_p, max_p + 1))
    random.shuffle(ports)

    # cerrar listener antiguo si existe
    if session.passive_listener:
        try:
            session.passive_listener.close()
        except: pass
        session.passive_listener = None

    listener = None
    chosen_port = None
    for p in ports:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('', p))     # escuchar en todas las interfaces
            s.listen(1)
            listener = s
            chosen_port = p
            break
        except Exception:
            continue

    if listener is None:
        session.client_socket.sendall(b'421 PASV failed.\r\n')
        print("[PASV] No se pudo abrir puerto en el rango PASV.")
        return

    # Calcular IP anunciada (usar socket de control para obtener IP real)
    server_ip = get_advertised_ip_for_session(session.client_socket)
    print(f"[PASV] anunciando {server_ip}:{chosen_port} (listener en 0.0.0.0:{chosen_port})")
    ip_parts = server_ip.split('.')
    p1, p2 = chosen_port // 256, chosen_port % 256
    resp = f"227 Entering Passive Mode ({ip_parts[0]},{ip_parts[1]},{ip_parts[2]},{ip_parts[3]},{p1},{p2}).\r\n"
    session.client_socket.sendall(resp.encode())
    # Guardar listener en la sesión para que accept_passive_connection lo use después
    session.passive_listener = listener
    # NOTA: no hacemos accept() aquí
    return

def PORT(arg, session):
    """
    Comando PORT: El cliente nos dice a dónde conectarnos para enviar datos.
    Formato: h1,h2,h3,h4,p1,p2
    """
    if not arg:
        session.client_socket.send(b"501 Syntax error.\r\n")
        return

    parts = arg.strip().split(',')
    if len(parts) != 6:
        session.client_socket.send(b"501 Syntax error.\r\n")
        return

    try:
        # Reconstruir IP y Puerto
        ip_address = '.'.join(parts[:4])
        port = int(parts[4]) * 256 + int(parts[5])
        
        print(f"[PORT] Cliente solicita conexión activa a {ip_address}:{port}")

        # Cerrar socket anterior si existe
        close_data_socket(session)

        # Crear socket y conectar activamente
        dsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        dsock.settimeout(10) # Timeout para la conexión
        dsock.connect((ip_address, port))
        
        session.data_socket = dsock
        session.client_socket.send(b"200 PORT command successful.\r\n")
        
    except Exception as e:
        print(f"[PORT] Error conectando al cliente: {e}")
        session.client_socket.send(b"425 Can't open data connection.\r\n")

# --- TRANSFERENCIAS ---

def RETR(arg, session):
    if not arg:
        session.client_socket.send(b"501 Syntax error.\r\n")
        return
    
    # 1. Verificar conexión de datos (PASV o PORT)
    if not session.data_socket:
        conn = accept_passive_connection(session)
        if not conn:
            session.client_socket.send(b"425 Use PASV or PORT first.\r\n")
            return
    
    try:
        # Obtener ruta absoluta
        try:
            target_path = safe_path(session, arg)
        except PermissionError:
            session.client_socket.send(b"550 Access denied.\r\n")
            close_data_socket(session)
            return

        # 2. Consultar al líder
        cluster_comm = get_global_cluster_comm()
        if not cluster_comm:
            session.client_socket.send(b"421 Cluster unavailable.\r\n")
            close_data_socket(session)
            return

        response = ask_leader('RETR', {
            'path': target_path,
            'requester': cluster_comm.local_ip,
            'session_user': session.username
        })
        
        if response.get('status') != 'ok':
            session.client_socket.send(f"550 {response.get('message')}\r\n".encode())
            close_data_socket(session)
            return
            
        # Datos del líder
        source_node = response.get('source')
        file_size = response.get('size', 0)
        
        # 3. ENVIAR ÚNICO MENSAJE 150 - CRÍTICO
        if session.type == 'I':
            session.client_socket.send(f"150 Opening BINARY mode data connection for {arg} ({file_size} bytes).\r\n".encode())
        else:
            session.client_socket.send(f"150 Opening ASCII mode data connection for {arg} ({file_size} bytes).\r\n".encode())

        # 4. Transferencia de datos
        success = False
        
        if source_node == cluster_comm.local_ip:
            # Archivo local
            success = send_local_file(target_path, session)
        else:
            # Descargar de nodo remoto primero
            temp_path = f"{target_path}.retr_temp.{int(time.time())}"
            transfer = get_node_transfer()
            
            dl_success, _, msg = transfer.request_file_from_node(source_node, target_path, temp_path)
            
            if dl_success:
                success = send_local_file(temp_path, session)
                try:
                    os.remove(temp_path)
                except: pass
            else:
                print(f"[RETR] Error descargando de nodo remoto: {msg}")

        # 5. Cerrar conexión de datos
        close_data_socket(session)
        
        # 6. Liberar lock ASINCRÓNICAMENTE (no bloquear respuesta)
        def release_lock_async():
            try:
                ask_leader('RELEASE_LOCK', {
                    'path': target_path,
                    'requester': cluster_comm.local_ip,
                    'session_user': session.username
                })
            except Exception as e:
                print(f"[RETR] Error liberando lock: {e}")
        
        threading.Thread(target=release_lock_async, daemon=True).start()

        # 7. Enviar respuesta final ÚNICA
        if success:
            session.client_socket.send(b"226 Transfer complete.\r\n")
        else:
            session.client_socket.send(b"450 Transfer failed.\r\n")

    except Exception as e:
        print(f"[RETR] Excepción crítica: {e}")
        import traceback
        traceback.print_exc()
        close_data_socket(session)
        session.client_socket.send(b"451 Server Error.\r\n")

def STOR(arg, session, append=False, unique=False):
    if not arg:
        session.client_socket.send(b"501 Syntax error.\r\n")
        return
    
    if not session.data_socket:
        conn = accept_passive_connection(session)
        if not conn:
            session.client_socket.send(b"425 Use PASV or PORT first.\r\n")
            return
    
    try:
        # Determinar nombre final
        if unique:
            filename = os.path.basename(arg)
            final_filename = generate_unique_filename(session.current_dir, filename)
        else:
            final_filename = arg
        
        # Obtener ruta local
        try:
            local_target = safe_path(session, final_filename)
        except PermissionError:
            session.client_socket.send(b"550 Access denied.\r\n")
            close_data_socket(session)
            return
        
        # --- Permitir sobreescritura si no es directorio ---
        if os.path.exists(local_target):
            if os.path.isdir(local_target):
                 session.client_socket.send(b"550 Cannot overwrite a directory.\r\n")
                 close_data_socket(session)
                 return
            # Si es archivo, dejamos que open('wb') lo trunque/sobreescriba
        
        # Recibir a temporal
        temp_filename = f"{local_target}.tmp.{int(time.time())}"
        
        msg = b"150 Opening data connection.\r\n"
        session.client_socket.sendall(msg)
        
        with open(temp_filename, 'wb') as f:
            while True:
                chunk = session.data_socket.recv(BUFFER_SIZE)
                if not chunk: break
                f.write(chunk)
        
        close_data_socket(session)
        
        # --- Mover el archivo a su ubicación final ANTES de contactar al líder ---
        # Mover a final (Sobreescritura atómica)
        if os.path.exists(local_target):
            os.remove(local_target)
        os.rename(temp_filename, local_target)
        
        # Calcular hash/size DESPUÉS de mover (para asegurar que existe)
        file_size = os.path.getsize(local_target)
        file_hash = calculate_file_hash(local_target)
        
        # Consultar al líder
        cluster_comm = get_global_cluster_comm()
        if not cluster_comm:
            session.client_socket.send(b"550 Cluster not available.\r\n")
            return
        
        # Enviar al líder
        response = ask_leader('STOR', {
            'path': local_target,
            'size': file_size,
            'hash': file_hash,
            'requester': cluster_comm.local_ip,
            'session_user': session.username,
            'append': append
        })
        
        if response.get('status') != 'ok':
            # Si el líder rechaza, eliminar el archivo local
            if os.path.exists(local_target):
                os.remove(local_target)
            error_msg = response.get('message', 'Error')
            session.client_socket.send(f"550 {error_msg}\r\n".encode())
            return
        
        # Registrar y Notificar
        operation_id = response.get('operation_id')
        register_local_operation(operation_id, 'STOR', local_target, session.username)
        notify_leader_completion(operation_id, True, session)
        
        session.client_socket.sendall(b"226 Transfer complete.\r\n")
            
    except Exception as e:
        close_data_socket(session)
        print(f"[STOR] Error: {e}")
        session.client_socket.sendall(b"451 Server Error.\r\n")

def APPE(arg, session):
    if not arg:
        session.client_socket.send(b"501 Syntax error.\r\n")
        return

    if not session.data_socket:
        conn = accept_passive_connection(session)
        if not conn:
            session.client_socket.send(b"425 Use PASV or PORT first.\r\n")
            return

    try:
        try:
            target_path = safe_path(session, arg)
        except PermissionError:
            session.client_socket.send(b"550 Access denied.\r\n")
            close_data_socket(session)
            return

        # Crear nombre único para el delta
        delta_filename = f"{target_path}.delta.{int(time.time())}_{random.randint(1000,9999)}"
        
        session.client_socket.sendall(b"150 Opening data connection for APPEND.\r\n")
        
        # Recibir delta del cliente
        with open(delta_filename, 'wb') as f:
            while True:
                chunk = session.data_socket.recv(BUFFER_SIZE)
                if not chunk: break
                f.write(chunk)
        
        close_data_socket(session)
        
        delta_size = os.path.getsize(delta_filename)
        print(f"[APPE] Delta recibido: {delta_filename} ({delta_size} bytes)")
        
        # Consultar al líder
        cluster_comm = get_global_cluster_comm()
        if not cluster_comm:
            os.remove(delta_filename)
            session.client_socket.send(b"550 Cluster not available.\r\n")
            return
            
        response = ask_leader('APPE', {
            'path': target_path,
            'size': delta_size,
            'requester': cluster_comm.local_ip,
            'session_user': session.username
        })
        
        if response.get('status') != 'ok':
            os.remove(delta_filename)
            error_msg = response.get('message', 'Error')
            session.client_socket.send(f"550 {error_msg}\r\n".encode())
            return
            
        replicas = response.get('replicas', [])
        operation_id = response.get('operation_id')
        lock_id = response.get('lock_id')
        
        print(f"[APPE] Coordinando append a {len(replicas)} réplicas: {replicas}")
        
        # Si yo soy una réplica, hacer append local
        if cluster_comm.local_ip in replicas:
            try:
                with open(target_path, 'ab') as target, open(delta_filename, 'rb') as delta:
                    target.write(delta.read())
                print(f"[APPE] Append local completado en {target_path}")
            except Exception as e:
                print(f"[APPE] Error en append local: {e}")

        # Identificar réplicas remotas
        remote_replicas = [ip for ip in replicas if ip != cluster_comm.local_ip]
        
        if remote_replicas:
            # Registrar delta transfer ANTES de enviar órdenes
            register_delta_transfer(delta_filename, remote_replicas)
            
            # Enviar órdenes a réplicas remotas
            for node_ip in remote_replicas:
                msg = {
                    'type': 'FS_ORDER',
                    'command': 'APPEND_BLOCK',
                    'path': target_path,
                    'delta_source': cluster_comm.local_ip,
                    'delta_path': delta_filename,
                    'operation_id': operation_id,
                    'requester': cluster_comm.local_ip
                }
                
                # Enviar sin esperar respuesta (non-blocking)
                threading.Thread(
                    target=cluster_comm.send_message,
                    args=(node_ip, msg, False),
                    daemon=True
                ).start()
            
            # Iniciar limpieza en hilo separado con tracking
            threading.Thread(
                target=cleanup_delta_safe,
                args=(delta_filename, remote_replicas, 90),  # 90s timeout
                daemon=True
            ).start()
        else:
            # No hay réplicas remotas, eliminar inmediatamente
            print(f"[APPE] No hay réplicas remotas, eliminando delta inmediatamente")
            try:
                os.remove(delta_filename)
            except Exception as e:
                print(f"[APPE] Error eliminando delta: {e}")
        
        # Notificar al líder que la operación del cliente terminó
        notify_leader_completion(operation_id, True, session)
        
        # Liberar el lock
        try:
            if lock_id:
                release_response = ask_leader('RELEASE_LOCK', {
                    'path': target_path,
                    'requester': cluster_comm.local_ip,
                    'session_user': session.username
                })
                if release_response.get('status') != 'ok':
                    print(f"[APPE] ⚠️ Fallo liberación de lock: {release_response.get('message')}")
        except Exception as e:
            print(f"[APPE] Error al liberar lock: {e}")

        session.client_socket.sendall(b"226 Append successful.\r\n")

    except Exception as e:
        close_data_socket(session)
        print(f"[APPE] Error: {e}")
        import traceback
        traceback.print_exc()
        session.client_socket.sendall(b"451 Server Error.\r\n")

def STOU(arg, session):
    filename = arg if arg else "file"
    return STOR(filename, session, append=False, unique=True)

def LIST(session):
    """Lista el contenido del directorio actual consultando al líder."""
    if not session.authenticated:
        session.client_socket.send(b"530 Not logged in.\r\n")
        return
    
    # Preparar conexión de datos
    if not session.data_socket:
        conn = accept_passive_connection(session)
        if not conn:
            session.client_socket.send(b"425 Use PASV or PORT first.\r\n")
            return
    
    # Consultar al líder
    cluster_comm = get_global_cluster_comm()
    if not cluster_comm:
        session.client_socket.send(b"550 Cluster not available.\r\n")
        close_data_socket(session)
        return
    
    response = ask_leader('LIST', {
        'abs_path': session.current_dir,
        'user_root': session.root_dir,
        'session_user': session.username
    })
    
    if response.get('status') != 'ok':
        error_msg = response.get('message', 'Error')
        session.client_socket.send(f"550 {error_msg}\r\n".encode())
        close_data_socket(session)
        return
    
    items = response.get('items', [])
    
    # Enviar lista formateada al cliente
    try:
        session.client_socket.sendall(b"150 Opening data connection for file list.\r\n")
        for item in items:
            # Formatear según el tipo (directorio o archivo)
            if item.get('type') == 'dir':
                file_details = f"drwxr-xr-x   1 user group {item.get('size', 0)} {time.strftime('%b %d %H:%M', time.localtime(item.get('mtime', time.time())))} {item.get('name', 'unknown')}\r\n"
            else:
                file_details = f"-rw-r--r--   1 user group {item.get('size', 0)} {time.strftime('%b %d %H:%M', time.localtime(item.get('mtime', time.time())))} {item.get('name', 'unknown')}\r\n"
            session.data_socket.sendall(file_details.encode())
        close_data_socket(session)
        session.client_socket.sendall(b"226 Transfer complete.\r\n")
    except Exception as e:
        print(f"[LIST] Error sending list: {e}")
        close_data_socket(session)
        session.client_socket.sendall(b"451 Requested action aborted: local error in processing.\r\n")

def NLST(session):
    """Lista solo nombres del directorio actual consultando al líder."""
    if not session.authenticated:
        session.client_socket.send(b"530 Not logged in.\r\n")
        return
    
    if not session.data_socket:
        conn = accept_passive_connection(session)
        if not conn:
            session.client_socket.send(b"425 Use PASV or PORT first.\r\n")
            return
    
    cluster_comm = get_global_cluster_comm()
    if not cluster_comm:
        session.client_socket.send(b"550 Cluster not available.\r\n")
        close_data_socket(session)
        return
    
    response = ask_leader('NLST', {
        'abs_path': session.current_dir,
        'user_root': session.root_dir,
        'session_user': session.username
    })
    
    if response.get('status') != 'ok':
        error_msg = response.get('message', 'Error')
        session.client_socket.send(f"550 {error_msg}\r\n".encode())
        close_data_socket(session)
        return
    
    names = response.get('names', [])
    
    try:
        session.client_socket.sendall(b"150 Opening data connection for file list.\r\n")
        for name in names:
            session.data_socket.sendall(f"{name}\r\n".encode())
        close_data_socket(session)
        session.client_socket.sendall(b"226 Transfer complete.\r\n")
    except Exception as e:
        print(f"[NLST] Error sending names: {e}")
        close_data_socket(session)
        session.client_socket.sendall(b"451 Requested action aborted: local error in processing.\r\n")

def ABOR(session):
    close_data_socket(session)
    session.client_socket.sendall(b"426 Aborted.\r\n")

