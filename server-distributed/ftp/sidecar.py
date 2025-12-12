import os
import shutil
import threading
import time
from ftp.alias_discovery import start_alias_discovery
from ftp.cluster_comm import start_cluster_communication
from ftp.state_manager import get_state_manager
from ftp.bully_election import BullyElection
from ftp.leader_operations import process_local_leader_request, get_leader_operations
from ftp.node_transfer import get_node_transfer

# Variables globales
_cluster_comm_global = None
_bully_instance = None

def start_sidecar():
    global _cluster_comm_global, _bully_instance
    print(">>> Iniciando Sidecar...")
    node_id = os.environ.get('NODE_ID', 'unknown')
    
    discovery = start_alias_discovery()
    initial_ips = discovery.get_cluster_ips()
    _cluster_comm_global = start_cluster_communication(node_id, initial_ips)
    state_mgr = get_state_manager(node_id)

    # Iniciar Bully
    local_ip = _cluster_comm_global.local_ip
    _bully_instance = BullyElection(node_id, local_ip, _cluster_comm_global, state_mgr)
    
    # Registrar handlers
    _cluster_comm_global.register_handler('ELECTION', _bully_instance.handle_election_msg)
    _cluster_comm_global.register_handler('COORDINATOR', _bully_instance.handle_coordinator_msg)
    _cluster_comm_global.register_handler('REQUEST_LOGS', handle_request_logs)
    _cluster_comm_global.register_handler('SYNC_COMMANDS', handle_sync_commands)
    _cluster_comm_global.register_handler('FS_REQUEST', handle_fs_request)
    _cluster_comm_global.register_handler('FS_ORDER', handle_fs_order)
    _cluster_comm_global.register_handler('MKD_NOTIFY', handle_mkd_notify)
    _cluster_comm_global.register_handler('REPLICA_SUCCESS', handle_replica_success)
    _cluster_comm_global.register_handler('REQUEST_DISK_SCAN', handle_request_disk_scan)
    _cluster_comm_global.register_handler('DELTA_CONFIRMATION', handle_delta_confirmation)

    discovery.set_ips_change_callback(update_ips_callback)
    if initial_ips: update_ips_callback(initial_ips)
    
    get_node_transfer(local_ip) # Iniciar transfer server
    
    if _bully_instance.am_i_leader():
        get_leader_operations(_cluster_comm_global, _bully_instance)

    # Iniciar el verificador de replicación en un hilo separado
    threading.Thread(target=start_replication_checker, daemon=True).start()
    threading.Thread(target=start_periodic_zombie_cleanup, daemon=True).start()

    return _cluster_comm_global

# --- HANDLERS PRINCIPALES ---

def handle_fs_order(message):
    """Este nodo (actuando como RÉPLICA) recibe una orden del líder."""
    
    command = message.get('command')
    path = message.get('path')
    operation_id = message.get('operation_id')
    check_empty = message.get('check_empty', False)
    
    state_mgr = get_state_manager()
    
    try:
        if command == 'CREATE_DIR':
            create_and_log_dirs(
                path,
                state_mgr,
                _cluster_comm_global.local_ip,
                operation_id
            )
            return {'status': 'ok', 'message': 'Directory created'}
            
        elif command == 'DELETE_DIR':
            # Función auxiliar para identificar archivos basura internos
            def _is_junk_file(filename):
                # Patrones de archivos temporales creados por APPE o transferencias
                return '.delta.' in filename or '.tmp.' in filename or filename.endswith('.tmp')

            try:
                if os.path.exists(path) and os.path.isdir(path):
                    # 1. Limpieza proactiva de artefactos internos (Deltas huerfanos)
                    # Si el usuario ya borró los archivos reales, estos deltas sobran.
                    try:
                        for f in os.listdir(path):
                            if _is_junk_file(f):
                                full_junk_path = os.path.join(path, f)
                                try:
                                    os.remove(full_junk_path)
                                    print(f"[ORDER] 🧹 Limpieza automática de artefacto interno: {f}")
                                except OSError:
                                    pass # Si no se puede borrar (bloqueado), lo dejamos y fallará el rmdir
                    except Exception as e:
                        print(f"[ORDER] Advertencia durante limpieza de deltas: {e}")

                    # 2. Verificar si está vacío (ahora ignorando los deltas que acabamos de borrar)
                    if check_empty and os.listdir(path):
                        # Si todavía queda algo (archivos reales del usuario), error.
                        return {'status': 'error', 'message': 'Directory not empty'}
                    
                    # 3. Borrar directorio
                    os.rmdir(path)
                    print(f"[ORDER] Directorio eliminado: {path}")
                    
                    state_mgr.append_operation('RMD', path, {
                        'operation_id': operation_id,
                        'executed_by': _cluster_comm_global.local_ip,
                        'from_order': True
                    })
                    
                    return {'status': 'ok', 'message': 'Directory deleted'}
                else:
                    # Idempotencia: Si ya no existe, damos OK
                    state_mgr.append_operation('RMD', path, {
                        'operation_id': operation_id,
                        'executed_by': _cluster_comm_global.local_ip,
                        'already_gone': True
                    })
                    return {'status': 'ok', 'message': 'Directory already removed'}
                    
            except Exception as e:
                print(f"[ORDER] Error eliminando directorio {path}: {e}")
                return {'status': 'error', 'message': str(e)}
            
        elif command == 'APPEND_BLOCK':
            delta_source = message.get('delta_source')
            delta_remote_path = message.get('delta_path')
            target_path = message.get('path')
            requester = message.get('requester')
            
            print(f"[SIDECAR] Orden APPEND_BLOCK recibida para {target_path}")
            print(f"[SIDECAR]   - Delta source: {delta_source}")
            print(f"[SIDECAR]   - Delta path: {delta_remote_path}")
            
            if not os.path.exists(target_path):
                print(f"[SIDECAR] Error APPEND: No tengo el archivo original {target_path}")
                return {'status': 'error', 'message': 'Original file missing'}
            
            # Descargar el delta
            local_delta_temp = target_path + f".delta_incoming.{time.time()}"
            
            from ftp.node_transfer import get_node_transfer
            transfer = get_node_transfer()
            
            print(f"[SIDECAR] Descargando delta desde {delta_source}...")
            success, _, msg = transfer.request_file_from_node(
                delta_source, delta_remote_path, local_delta_temp
            )
            
            if success:
                try:
                    # Hacer append
                    print(f"[SIDECAR] Aplicando append en {target_path}...")
                    with open(target_path, 'ab') as target, open(local_delta_temp, 'rb') as delta:
                        while True:
                            chunk = delta.read(1024*1024)
                            if not chunk: break
                            target.write(chunk)
                    
                    print(f"[SIDECAR] Append aplicado exitosamente en {target_path}")
                    
                    # Limpiar delta temporal
                    os.remove(local_delta_temp)
                    
                    # Enviar confirmación al nodo que envió la orden
                    try:
                        print(f"[SIDECAR] Enviando confirmación de append a {requester}")
                        confirmation_msg = {
                            'type': 'DELTA_CONFIRMATION',
                            'delta_path': delta_remote_path,
                            'node_ip': _cluster_comm_global.local_ip,
                            'target_path': target_path,
                            'status': 'success',
                            'timestamp': time.time()
                        }
                        
                        # Enviar confirmación (non-blocking)
                        threading.Thread(
                            target=_cluster_comm_global.send_message,
                            args=(requester, confirmation_msg, False),
                            daemon=True
                        ).start()
                        
                    except Exception as e:
                        print(f"[SIDECAR] Error enviando confirmación: {e}")
                    
                    return {'status': 'ok'}
                    
                except Exception as e:
                    print(f"[SIDECAR] Error escribiendo append: {e}")
                    # Intentar limpiar delta temporal
                    try:
                        if os.path.exists(local_delta_temp):
                            os.remove(local_delta_temp)
                    except:
                        pass
                    return {'status': 'error', 'message': str(e)}
            else:
                print(f"[SIDECAR] Fallo descargando delta desde {delta_source}: {msg}")
                return {'status': 'error', 'message': f'Failed to download delta: {msg}'}

        elif command == 'RENAME':
            old_path = message.get('old_path')
            new_path = message.get('new_path')
            operation_id = message.get('operation_id')
            item_type = message.get('item_type', 'file')
            is_directory = message.get('is_directory', False)
            must_execute_on_all = message.get('must_execute_on_all', False)
            
            print(f"[ORDER] RENAME recibido: {old_path} -> {new_path} (tipo: {item_type}, directorio: {is_directory})")
            
            # Si es un directorio, DEBEMOS ejecutarlo aunque no tengamos el archivo
            if is_directory and must_execute_on_all:
                print(f"[ORDER] Directorio - EJECUTANDO en todos los nodos: {old_path} -> {new_path}")
            
            try:
                # Para directorios, manejamos de forma especial
                if is_directory:
                    success = _handle_directory_rename(old_path, new_path, operation_id)
                    
                    # CRÍTICO: Después de renombrar, limpiar cualquier estructura antigua que haya quedado
                    if success and os.path.exists(old_path):
                        print(f"[ORDER] Estructura antigua detectada después de renombrado: {old_path}")
                        try:
                            # Intentar eliminar la estructura antigua
                            if os.path.isdir(old_path):
                                # Verificar si tiene contenido
                                contents = os.listdir(old_path)
                                if contents:
                                    print(f"[ORDER] Estructura antigua tiene {len(contents)} elementos, limpiando...")
                                    shutil.rmtree(old_path)
                                    print(f"[ORDER] Estructura antigua eliminada: {old_path}")
                                else:
                                    os.rmdir(old_path)
                                    print(f"[ORDER] Directorio vacío antiguo eliminado: {old_path}")
                        except Exception as cleanup_error:
                            print(f"[ORDER] No se pudo limpiar estructura antigua: {cleanup_error}")
                            # No fallar la operación por esto
                else:
                    success = _handle_file_rename(old_path, new_path, operation_id, message)
                
                if success:
                    # Actualizar nuestro state manager local
                    state_mgr = get_state_manager()
                    with state_mgr.lock:
                        # Si tenemos entrada para el archivo/directorio viejo, moverla
                        if old_path in state_mgr.file_map:
                            state_mgr.file_map[new_path] = state_mgr.file_map[old_path]
                            del state_mgr.file_map[old_path]
                            
                            # Si es un directorio, actualizar rutas hijas
                            if is_directory:
                                old_prefix = old_path.rstrip('/') + '/'
                                new_prefix = new_path.rstrip('/') + '/'
                                
                                keys_to_update = [k for k in list(state_mgr.file_map.keys()) 
                                                if k.startswith(old_prefix)]
                                
                                for old_key in keys_to_update:
                                    relative_part = old_key[len(old_prefix):]
                                    new_key = new_prefix + relative_part
                                    
                                    if old_key in state_mgr.file_map:
                                        state_mgr.file_map[new_key] = state_mgr.file_map[old_key]
                                        del state_mgr.file_map[old_key]
                    
                    # Registrar en log local
                    state_mgr.append_operation('RN', new_path, {
                        'old_path': old_path,
                        'operation_id': operation_id,
                        'executed_by': _cluster_comm_global.local_ip,
                        'from_order': True,
                        'item_type': item_type,
                        'success': success
                    })
                    
                    return {'status': 'ok', 'message': 'Renamed successfully'}
                
                else:
                    # SI FALLÓ (por busy u otro), NO tocamos el mapa local y avisamos error
                    return {'status': 'error', 'message': 'Rename failed on disk (File Busy or Error)'}
                
            except Exception as e:
                print(f"[ORDER] Error en RENAME {old_path} -> {new_path}: {e}")
                import traceback
                traceback.print_exc()
                
                # Registrar error en log
                state_mgr = get_state_manager()
                state_mgr.append_operation('RN', new_path, {
                    'old_path': old_path,
                    'operation_id': operation_id,
                    'executed_by': _cluster_comm_global.local_ip,
                    'error': str(e),
                    'failed': True
                })
                
                return {'status': 'error', 'message': str(e)}

        elif command == 'DELETE_FILE':
            try:
                if os.path.exists(path) and os.path.isfile(path):
                    os.remove(path)
                    print(f"[ORDER] Archivo eliminado: {path}")
                    
                    state_mgr.append_operation('DELE', path, {
                        'operation_id': operation_id,
                        'executed_by': _cluster_comm_global.local_ip,
                        'from_order': True
                    })
                    
                    return {'status': 'ok', 'message': 'File deleted'}
                else:
                    # Si no existe, aún así registrar la operación
                    state_mgr.append_operation('DELE', path, {
                        'operation_id': operation_id,
                        'executed_by': _cluster_comm_global.local_ip,
                        'already_gone': True
                    })
                    return {'status': 'ok', 'message': 'File already removed'}
                    
            except Exception as e:
                print(f"[ORDER] Error eliminando archivo {path}: {e}")
                return {'status': 'error', 'message': str(e)}    

        elif command == 'REPLICATE_FILE':
            source_ip = message.get('source')
            file_size = message.get('size')
            file_hash = message.get('hash')
            is_repair = message.get('is_repair', False)
            repair_type = message.get('repair_type', '')
            failed_node = message.get('failed_node', '')
            from_leader = message.get('from_leader', False)
            operation_id = message.get('operation_id', '')
            
            print(f"[SIDECAR] Orden REPLICATE_FILE recibida para {path} desde {source_ip}")
            
            # 1. Asegurar directorios
            dir_path = os.path.dirname(path)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)
            
            # 2. Transferir archivo
            from ftp.node_transfer import get_node_transfer
            transfer = get_node_transfer()
            
            max_retries = 3
            for attempt in range(max_retries):
                success, actual_path, msg = transfer.request_file_from_node(source_ip, path, path)
                
                if success:
                    # 3. Registrar operación STOR en el log local
                    metadata = {
                        'size': file_size,
                        'hash': file_hash,
                        'replicas': [source_ip, _cluster_comm_global.local_ip],
                        'operation_id': operation_id,
                        'from_replication': True,
                        'timestamp': time.time(),
                        'source_node': source_ip
                    }
                    
                    # CORRECCIÓN: Usar state_mgr que ya tenemos
                    state_mgr.append_operation('STOR', path, metadata)
                    
                    # 4. Actualizar file_map local
                    if path not in state_mgr.file_map:
                        state_mgr.file_map[path] = {
                            'type': 'file',
                            'size': file_size,
                            'hash': file_hash,
                            'replicas': [source_ip, _cluster_comm_global.local_ip],
                            'mtime': time.time()
                        }
                    else:
                        # Asegurar que estamos en la lista de réplicas
                        if _cluster_comm_global.local_ip not in state_mgr.file_map[path].get('replicas', []):
                            state_mgr.file_map[path]['replicas'].append(_cluster_comm_global.local_ip)
                    
                    print(f"[SIDECAR] Réplica exitosa: {path}")
                    
                    # 5. Notificar al líder si es reparación
                    if is_repair and not from_leader and _bully_instance and not _bully_instance.am_i_leader():
                        leader_ip = _bully_instance.get_leader()
                        if leader_ip:
                            try:
                                notify_msg = {
                                    'type': 'REPLICA_SUCCESS',
                                    'path': path,
                                    'node_ip': _cluster_comm_global.local_ip,
                                    'timestamp': time.time(),
                                    'repair_type': repair_type,
                                    'failed_node': failed_node
                                }
                                _cluster_comm_global.send_message(leader_ip, notify_msg, expect_response=False)
                            except Exception as e:
                                print(f"[SIDECAR] Error notificando réplica exitosa: {e}")
                    
                    return {'status': 'ok', 'message': 'File replicated'}
                else:
                    print(f"[SIDECAR] Intento {attempt + 1} falló para {path}: {msg}")
                    if attempt < max_retries - 1:
                        time.sleep(1)
            
            print(f"[SIDECAR] Fallo en réplica de {path} después de {max_retries} intentos")
            return {'status': 'error', 'message': f'Replication failed after {max_retries} attempts'}
        
        else:
            print(f"[ORDER] Comando desconocido: {command}")
            return {'status': 'error', 'message': f'Unknown command: {command}'}

    except Exception as e:
        print(f"[ORDER] Error: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}

def handle_fs_request(message):
    """
    Este nodo (actuando como LÍDER) recibe una petición de un cliente (via otro nodo).
    """
    msg_type = message.get('subtype')
    data = message.get('data', {})
    # Esta función llama a LeaderOperations
    return process_local_leader_request(msg_type, data)

def handle_mkd_notify(message):
    """
    Mensaje esperado: {'path': <abs_path>, 'node_ip': <ip>}
    El líder añade la IP como replica de forma inmediata.
    """
    try:
        path = message.get('path')
        node_ip = message.get('node_ip')
        sm = get_state_manager()
        with sm.lock:
            # Aseguramos entrada en file_map
            if path not in sm.file_map:
                sm.file_map[path] = {
                    'type': 'dir',
                    'mtime': time.time(),
                    'replicas': []
                }
            # Añadir si no estaba
            if node_ip not in sm.file_map[path]['replicas']:
                sm.file_map[path]['replicas'].append(node_ip)
                print(f"[LIDER] MKD_NOTIFY -> añadido replica {node_ip} para {path}")
        return {'status': 'ok'}
    except Exception as e:
        print(f"[LIDER] Error en handle_mkd_notify: {e}")
        return {'status': 'error', 'message': str(e)}

def handle_request_logs(message):
    """El nuevo líder pide mis logs para reconstruir el esquema."""
    sm = get_state_manager()
    return {
        'status': 'ok', 
        'log': sm.get_full_log_as_dict(),
        'node_ip': _cluster_comm_global.local_ip
    }

def handle_sync_commands(message):
    """
    Ejecuta comandos de sincronización enviados por el líder.
    """
    commands = message.get('commands', [])
    print(f"[SIDECAR] Recibidos {len(commands)} comandos de sincronización")
    
    results = []
    
    for cmd in commands:
        command = cmd.get('command')
        path = cmd.get('path')
        reason = cmd.get('reason', '')
        
        try:
            if command == 'DELETE_FILE':
                if os.path.exists(path) and os.path.isfile(path):
                    os.remove(path)
                    print(f"[SYNC] Zombi eliminado: {path}")
                    # Registrar en log local para no volver a resucitarlo si reiniciamos
                    get_state_manager().append_operation('DELE', path, {'zombie_purge': True})
                    results.append({
                        'command': command,
                        'path': path,
                        'status': 'deleted',
                        'reason': reason
                    })
                else:
                    results.append({
                        'command': command,
                        'path': path,
                        'status': 'already_gone'
                    })
    
            elif command == 'CREATE_FILE' or command == 'UPDATE_FILE':
                # CREACIÓN/ACTUALIZACIÓN DE ARCHIVO CON LAST WRITE WINS
                replicas = cmd.get('replicas', [])
                expected_size = cmd.get('size', 0)
                expected_hash = cmd.get('hash', '')
                global_mtime = cmd.get('global_mtime', 0)
                
                # Excluir al nodo local de la lista de réplicas
                replicas = [ip for ip in replicas if ip != _cluster_comm_global.local_ip]
                
                if not replicas:
                    print(f"[SYNC] {command} {path} - No hay réplicas disponibles")
                    results.append({
                        'command': command, 
                        'path': path, 
                        'status': 'error', 
                        'error': 'No replicas available'
                    })
                    continue
                
                # Verificar si ya existe y comparar timestamps
                if os.path.exists(path):
                    existing_mtime = os.path.getmtime(path)
                    existing_size = os.path.getsize(path)
                    
                    print(f"[SYNC] {command} {path} - Archivo existe (mtime local: {existing_mtime}, global: {global_mtime})")
                    
                    # Last Write Wins: Solo reemplazar si el global es MÁS RECIENTE
                    if command == 'UPDATE_FILE' and global_mtime > 0 and existing_mtime >= global_mtime:
                        print(f"[SYNC] {command} {path} - Versión local es igual o más reciente. CONSERVANDO local.")
                        results.append({
                            'command': command,
                            'path': path,
                            'status': 'kept_local',
                            'reason': 'local_version_newer_or_equal',
                            'local_mtime': existing_mtime,
                            'global_mtime': global_mtime
                        })
                        continue
                    
                    # Si es CREATE_FILE y el archivo existe, conservarlo (no debería pasar)
                    if command == 'CREATE_FILE':
                        print(f"[SYNC] {command} {path} - Archivo ya existe. CONSERVANDO (no debería pasar).")
                        results.append({
                            'command': command,
                            'path': path,
                            'status': 'already_exists',
                            'size': existing_size
                        })
                        continue
                    
                    # Si llegamos aquí, el global es más reciente - proceder con actualización
                    print(f"[SYNC] {command} {path} - Versión global más reciente. Actualizando...")
                
                print(f"[SYNC] {command} {path} - Descargando de réplica (Razón: {reason})")
                
                success = False
                last_error = ""
                
                for replica_ip in replicas:
                    try:
                        from ftp.node_transfer import get_node_transfer
                        transfer = get_node_transfer()
                        
                        # Descargar a temporal primero
                        temp_path = f"{path}.tmp.{int(time.time())}"
                        success, actual_path, msg = transfer.request_file_from_node(replica_ip, path, temp_path)
                        
                        if success:
                            # Verificar tamaño
                            if expected_size > 0:
                                actual_size = os.path.getsize(temp_path)
                                if actual_size != expected_size:
                                    print(f"[SYNC] {command} {path} - Tamaño incorrecto: esperado={expected_size}, actual={actual_size}")
                                    os.remove(temp_path)
                                    success = False
                                    last_error = f"Size mismatch: {actual_size} vs {expected_size}"
                                    continue
                            
                            # Mover temporal a ubicación final
                            if os.path.exists(path):
                                os.remove(path)
                            os.rename(temp_path, path)
                            
                            print(f"[SYNC] {command} {path} - Descargado exitosamente desde {replica_ip}")
                            break
                        else:
                            print(f"[SYNC] {command} {path} - Falló descarga desde {replica_ip}: {msg}")
                            last_error = msg
                    except Exception as e:
                        print(f"[SYNC] {command} {path} - Error con réplica {replica_ip}: {e}")
                        last_error = str(e)
                
                if success:
                    # Registrar en el log local
                    state_mgr = get_state_manager()
                    metadata = {
                        'size': expected_size,
                        'hash': expected_hash,
                        'from_sync': True,
                        'reason': reason,
                        'source_replica': replica_ip if success else None,
                        'command_type': command
                    }
                    
                    if command == 'UPDATE_FILE':
                        metadata['global_mtime'] = global_mtime
                    
                    state_mgr.append_operation('STOR', path, metadata)
                    
                    results.append({
                        'command': command, 
                        'path': path, 
                        'status': 'created' if command == 'CREATE_FILE' else 'updated',
                        'source': replica_ip if success else None
                    })
                else:
                    print(f"[SYNC] {command} {path} - No se pudo descargar de ninguna réplica")
                    results.append({
                        'command': command, 
                        'path': path, 
                        'status': 'error', 
                        'error': f'Failed to download from any replica: {last_error}'
                    })
                
            elif command == 'DELETE_DIR':
                # ELIMINACIÓN DE DIRECTORIO ZOMBI
                if os.path.exists(path) and os.path.isdir(path):
                    # Intentar eliminar solo si está vacío
                    try:
                        # Listar contenido
                        contents = os.listdir(path)
                        if contents:
                            print(f"[SYNC] DELETE_DIR {path} - No vacío ({len(contents)} elementos), no se puede eliminar")
                            results.append({
                                'command': command, 
                                'path': path, 
                                'status': 'not_empty',
                                'item_count': len(contents)
                            })
                        else:
                            os.rmdir(path)
                            print(f"[SYNC] DELETE_DIR {path} - eliminado zombi (Razón: {reason})")
                            
                            # Registrar eliminación en log local
                            state_mgr = get_state_manager()
                            state_mgr.append_operation('RMD', path, {
                                'from_sync': True,
                                'reason': reason,
                                'was_zombie': True,
                                'was_empty': True
                            })
                            
                            results.append({
                                'command': command, 
                                'path': path, 
                                'status': 'deleted',
                                'reason': reason
                            })
                    except OSError as e:
                        print(f"[SYNC] DELETE_DIR {path} - Error: {e}")
                        results.append({
                            'command': command, 
                            'path': path, 
                            'status': 'error', 
                            'error': str(e)
                        })
                else:
                    print(f"[SYNC] DELETE_DIR {path} - no existía")
                    results.append({
                        'command': command, 
                        'path': path, 
                        'status': 'already_gone',
                        'reason': reason
                    })

            elif command == 'DELETE_DIR_RECURSIVE':
                # Eliminación recursiva de directorios obsoletos
                if os.path.exists(path) and os.path.isdir(path):
                    try:
                        print(f"[SYNC] Eliminando directorio obsoleto recursivamente: {path}")
                        print(f"[SYNC] Razón: {reason}")
                        
                        # Contar archivos antes de eliminar
                        file_count = sum(len(files) for _, _, files in os.walk(path))
                        
                        if file_count > 0:
                            print(f"[SYNC] Directorio contiene {file_count} archivos (todos obsoletos)")
                        
                        # Eliminar recursivamente
                        shutil.rmtree(path)
                        
                        print(f"[SYNC] Directorio obsoleto eliminado: {path} ({file_count} archivos)")
                        
                        # Registrar en log local
                        get_state_manager().append_operation('RMD', path, {
                            'zombie_purge': True,
                            'recursive': True,
                            'reason': reason,
                            'files_deleted': file_count
                        })
                        
                        results.append({
                            'command': command,
                            'path': path,
                            'status': 'deleted_recursive',
                            'files_deleted': file_count,
                            'reason': reason
                        })
                        
                    except Exception as e:
                        print(f"[SYNC] Error eliminando directorio recursivamente {path}: {e}")
                        results.append({
                            'command': command,
                            'path': path,
                            'status': 'error',
                            'error': str(e)
                        })
                else:
                    print(f"[SYNC] Directorio obsoleto {path} ya no existe")
                    results.append({
                        'command': command,
                        'path': path,
                        'status': 'already_gone'
                    })

            elif command == 'CREATE_DIR':
                # Creación de directorio (existente)
                os.makedirs(path, exist_ok=True)
                print(f"[SYNC] CREATE_DIR {path} - creado/verificado (Razón: {reason})")
                
                # Registrar en log local
                state_mgr = get_state_manager()
                state_mgr.append_operation('MKD', path, {
                    'from_sync': True,
                    'reason': reason
                })
                
                results.append({
                    'command': command, 
                    'path': path, 
                    'status': 'created'
                })
                
            else:
                print(f"[SYNC] Comando desconocido: {command}")
                results.append({
                    'command': command, 
                    'path': path, 
                    'status': 'unknown'
                })
                
        except Exception as e:
            print(f"[SYNC] Error ejecutando {command} {path}: {e}")
            import traceback
            traceback.print_exc()
            results.append({
                'command': command, 
                'path': path, 
                'status': 'error', 
                'error': str(e)
            })
    
    # Resumen de operaciones
    stats = {
        'total': len(results),
        'success': sum(1 for r in results if r['status'] in ['created', 'updated', 'deleted', 'deleted_recursive', 'already_gone', 'kept_local', 'already_exists']),
        'errors': sum(1 for r in results if r['status'] == 'error'),
        'zombies_deleted': sum(1 for r in results if 'zombie' in r.get('reason', '') and r['status'] in ['deleted', 'deleted_recursive']),
        'kept_local': sum(1 for r in results if r.get('status') == 'kept_local')
    }
    
    print(f"[SYNC] Resumen: {stats}")
    
    return {'status': 'ok', 'results': results, 'stats': stats}

def handle_delta_confirmation(message):
    """
    Maneja confirmaciones de que un nodo completó su append.
    """
    delta_path = message.get('delta_path')
    node_ip = message.get('node_ip')
    status = message.get('status')
    target_path = message.get('target_path')
    
    print(f"[DELTA-CONFIRM] Confirmación recibida de {node_ip} para {delta_path}")
    print(f"[DELTA-CONFIRM]   - Target: {target_path}")
    print(f"[DELTA-CONFIRM]   - Status: {status}")

    from ftp.commands import confirm_delta_transfer

    if status == 'success':
        # Marcar nodo como completado
        all_done = confirm_delta_transfer(delta_path, node_ip)
        
        if all_done:
            print(f"[DELTA-CONFIRM] Todas las réplicas confirmaron para {delta_path}")
        else:
            print(f"[DELTA-CONFIRM] Aún esperando confirmaciones de otros nodos")
    else:
        print(f"[DELTA-CONFIRM] Nodo {node_ip} reportó fallo en append")
        # Aún marcar como completado para no bloquear la limpieza
        confirm_delta_transfer(delta_path, node_ip)
    
    return {'status': 'ok'}

def handle_request_disk_scan(message):
    """
    Responde con un escaneo completo del sistema de archivos local.
    Esto permite al líder detectar zombies que el log no reporta.
    """
    try:
        root_path = message.get('root_path', '/app/server/data')
        
        print(f"[SIDECAR] Escaneando disco en {root_path}...")
        
        sm = get_state_manager()
        disk_scan = sm.scan_local_filesystem(root_path)
        
        print(f"[SIDECAR] Escaneo completado: {len(disk_scan)} elementos encontrados")
        
        return {
            'status': 'ok',
            'disk_scan': disk_scan,
            'scan_timestamp': time.time(),
            'root_path': root_path
        }
    except Exception as e:
        print(f"[SIDECAR] Error en escaneo de disco: {e}")
        import traceback
        traceback.print_exc()
        return {
            'status': 'error',
            'message': str(e)
        }

def handle_replica_success(message):
        """Actualiza el estado global cuando una réplica es exitosa."""
        path = message.get('path')
        node_ip = message.get('node_ip')
        
        if _bully_instance and _bully_instance.am_i_leader():
            state_mgr = get_state_manager()
            with state_mgr.lock:
                if path in state_mgr.file_map:
                    if node_ip not in state_mgr.file_map[path].get('replicas', []):
                        state_mgr.file_map[path]['replicas'].append(node_ip)
                        print(f"[LIDER] Réplica añadida: {node_ip} para {path}")
        
        return {'status': 'ok'}

# --- HILOS SECUNDARIOS ---

def update_ips_callback(new_ips):
        global _cluster_comm_global, _bully_instance
        
        old_ips = set(_cluster_comm_global.cluster_ips)
        new_ips_set = set(new_ips)
        
        added_ips = new_ips_set - old_ips
        removed_ips = old_ips - new_ips_set
        
        _cluster_comm_global.update_cluster_ips(new_ips)
        _bully_instance.update_nodes(new_ips)
        
        # Si soy líder, gestionar fallos de nodos
        if _bully_instance and _bully_instance.am_i_leader():
            # Para cada nodo que ha salido, manejar su falla
            for failed_ip in removed_ips:
                print(f"[SIDECAR] Nodo {failed_ip} ha salido del cluster, iniciando recuperación...")
                handle_node_failure(failed_ip)
            
            # Para cada nodo que ha entrado, verificar si necesita réplicas
            for new_ip in added_ips:
                print(f"[SIDECAR] Nodo {new_ip} ha entrado, verificando réplicas necesarias...")
                # Llamar a la función correcta
                handle_node_join(new_ip)
        
        if added_ips:
            print(f"[SIDECAR] Nodos añadidos: {added_ips}")
        if removed_ips:
            print(f"[SIDECAR] Nodos removidos: {removed_ips}")

def start_replication_checker():
        """Verifica periódicamente el estado de replicación."""
        while True:
            time.sleep(60)  # Verificar cada 60 segundos
            
            if _bully_instance and _bully_instance.am_i_leader():
                ops = get_leader_operations()
                if ops:
                    ops.check_replication_status()

def start_periodic_zombie_cleanup():
    """
    Ejecuta garbage collection completo cada 5 minutos.
    Esto limpia archivos y directorios obsoletos en TODO el cluster.
    """
    while True:
        time.sleep(300)  # 5 minutos
        
        if _bully_instance and _bully_instance.am_i_leader():
            # Verificar que no estamos reconstruyendo
            if _bully_instance.is_reconstructing():
                print("[SIDECAR] GC diferido: Líder aún reconstruyendo estado")
                continue
            
            print("[SIDECAR] Ejecutando garbage collection periódico global...")
            
            try:
                from ftp.leader_operations import get_leader_operations
                ops = get_leader_operations()
                
                if ops:
                    ops.run_garbage_collection()
                else:
                    print("[SIDECAR] No se pudo obtener LeaderOperations para GC")
                    
            except Exception as e:
                print(f"[SIDECAR] Error en garbage collection periódico: {e}")
                import traceback
                traceback.print_exc()

