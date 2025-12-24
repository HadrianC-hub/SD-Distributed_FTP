import os
import threading
import time
from collections import defaultdict
from ftp.alias_discovery import start_alias_discovery
from ftp.cluster_comm import start_cluster_communication
from ftp.state_manager import get_state_manager
from ftp.bully_election import BullyElection
from ftp.leader_operations import process_local_leader_request, get_leader_operations

# Variables globales
_cluster_comm_global = None
_bully_instance = None
_last_known_cluster_size = 0
_nodes_being_integrated = set()
_integration_lock = threading.Lock()

# Cola de órdenes pendientes
pending_orders_queue = []
pending_orders_lock = threading.Lock()

def start_sidecar(node_id):
    global _cluster_comm_global, _bully_instance, _last_known_cluster_size
    print(">>> Iniciando Sidecar...")
    
    # Iniciando servicio de Alias-Discovery
    discovery = start_alias_discovery()
    initial_ips = discovery.get_cluster_ips()

    # Iniciando comunicaciones entre nodos
    _cluster_comm_global = start_cluster_communication(node_id, initial_ips)

    # Iniciando manejador de estado (logs de los nodos)
    state_mgr = get_state_manager(node_id)

    # Iniciar Bully
    local_ip = _cluster_comm_global.local_ip
    _bully_instance = BullyElection(node_id, local_ip, _cluster_comm_global, state_mgr)

    # Iniciar servicio de transferencias entre nodos
    from ftp.node_transfer import get_node_transfer
    get_node_transfer(local_ip)
    
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
    if initial_ips: 
        _last_known_cluster_size = len(initial_ips)
        update_ips_callback(initial_ips)
    
    if _bully_instance.am_i_leader():
        get_leader_operations(_cluster_comm_global, _bully_instance)

    threading.Thread(target=start_replication_checker, daemon=True).start()

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
            try:
                if os.path.exists(path) and os.path.isdir(path):
                    # MEJORADO: Eliminar recursivamente TODO el contenido
                    print(f"[ORDER] Eliminando directorio completo: {path}")
                    
                    # Listar todo el contenido antes de borrar
                    files_inside = []
                    dirs_inside = []
                    
                    for root, dirs, files in os.walk(path, topdown=False):
                        for file in files:
                            file_path = os.path.join(root, file)
                            files_inside.append(file_path)
                        for dir_name in dirs:
                            dir_path = os.path.join(root, dir_name)
                            dirs_inside.append(dir_path)
                    
                    if files_inside or dirs_inside:
                        print(f"[ORDER] Directorio contiene {len(files_inside)} archivos y {len(dirs_inside)} subdirectorios")
                    
                    # Eliminar archivos primero
                    for file_path in files_inside:
                        try:
                            if os.path.exists(file_path):
                                os.remove(file_path)
                                print(f"[ORDER]  Archivo eliminado: {file_path}")
                        except Exception as e:
                            print(f"[ORDER]  Error eliminando archivo {file_path}: {e}")
                    
                    # Eliminar subdirectorios
                    for dir_path in dirs_inside:
                        try:
                            if os.path.exists(dir_path) and os.path.isdir(dir_path):
                                if not os.listdir(dir_path):  # Solo si está vací­o
                                    os.rmdir(dir_path)
                                    print(f"[ORDER]  Subdirectorio eliminado: {dir_path}")
                        except Exception as e:
                            print(f"[ORDER]  Error eliminando subdirectorio {dir_path}: {e}")
                    
                    # Verificar si quedó vací­o
                    if check_empty and os.listdir(path):
                        remaining = os.listdir(path)
                        print(f"[ORDER]  Directorio no pudo vaciarse completamente. Quedan: {remaining}")
                        return {'status': 'error', 'message': 'Directory not empty after cleanup'}
                    
                    # Finalmente eliminar el directorio principal
                    os.rmdir(path)
                    print(f"[ORDER]  Directorio principal eliminado: {path}")
                    
                    state_mgr.append_operation('RMD', path, {
                        'operation_id': operation_id,
                        'executed_by': _cluster_comm_global.local_ip,
                        'from_order': True,
                        'files_deleted': len(files_inside),
                        'dirs_deleted': len(dirs_inside)
                    })
                    
                    return {'status': 'ok', 'message': 'Directory deleted completely'}
                else:
                    # No existe, pero registrar que ya fue eliminado
                    state_mgr.append_operation('RMD', path, {
                        'operation_id': operation_id,
                        'executed_by': _cluster_comm_global.local_ip,
                        'already_gone': True
                    })
                    return {'status': 'ok', 'message': 'Directory already removed'}
                    
            except Exception as e:
                print(f"[ORDER] Error eliminando directorio {path}: {e}")
                import traceback
                traceback.print_exc()
                return {'status': 'error', 'message': str(e)}
            
        elif command == 'APPEND_BLOCK':
            delta_source = message.get('delta_source')
            delta_remote_path = message.get('delta_path')
            target_path = message.get('path')
            requester = message.get('requester')
            
            print(f"[SIDECAR] Orden APPEND_BLOCK recibida para {target_path}")
            
            if not os.path.exists(target_path):
                print(f"[SIDECAR] Error APPEND: No tengo el archivo original {target_path}")
                return {'status': 'error', 'message': 'Original file missing'}
            
            # Guardar tamaño y hash antes del append
            size_before = os.path.getsize(target_path)
            
            local_delta_temp = target_path + f".delta_incoming.{time.time()}"
            
            from ftp.node_transfer import get_node_transfer
            transfer = get_node_transfer()
            
            print(f"[SIDECAR] Descargando delta desde {delta_source}...")
            success, _, msg = transfer.request_file_from_node(
                delta_source, delta_remote_path, local_delta_temp
            )
            
            if success:
                try:
                    print(f"[SIDECAR] Aplicando append en {target_path}...")
                    with open(target_path, 'ab') as target, open(local_delta_temp, 'rb') as delta:
                        while True:
                            chunk = delta.read(1024*1024)
                            if not chunk: break
                            target.write(chunk)
                    
                    size_after = os.path.getsize(target_path)
                    delta_applied = size_after - size_before
                    
                    print(f"[SIDECAR] Append aplicado exitosamente en {target_path}")
                    print(f"[SIDECAR] Tamaño antes: {size_before}, después: {size_after}, delta: {delta_applied}")
                    
                    os.remove(local_delta_temp)
                    
                    # Registrar en el log
                    state_mgr.append_operation('APPE', target_path, {
                        'delta_size': delta_applied,
                        'from_order': True,
                        'operation_id': operation_id,
                        'source_node': delta_source,
                        'final_size': size_after
                    })
                    
                    try:
                        confirmation_msg = {
                            'type': 'DELTA_CONFIRMATION',
                            'delta_path': delta_remote_path,
                            'node_ip': _cluster_comm_global.local_ip,
                            'target_path': target_path,
                            'status': 'success',
                            'timestamp': time.time(),
                            'delta_applied': delta_applied,
                            'final_size': size_after
                        }
                        
                        threading.Thread(
                            target=_cluster_comm_global.send_message,
                            args=(requester, confirmation_msg, False),
                            daemon=True
                        ).start()
                        
                    except Exception as e:
                        print(f"[SIDECAR] Error enviando confirmación: {e}")
                    
                    return {'status': 'ok', 'final_size': size_after}
                    
                except Exception as e:
                    print(f"[SIDECAR] Error escribiendo append: {e}")
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
            
            print(f"[ORDER] RENAME recibido: {old_path} -> {new_path} (tipo: {item_type})")
            
            try:
                if is_directory:
                    success = _handle_directory_rename(old_path, new_path, operation_id)
                else:
                    success = _handle_file_rename(old_path, new_path, operation_id, message)
                
                if success:
                    state_mgr = get_state_manager()
                    with state_mgr.lock:
                        if old_path in state_mgr.file_map:
                            state_mgr.file_map[new_path] = state_mgr.file_map[old_path]
                            del state_mgr.file_map[old_path]
                            
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
                    return {'status': 'error', 'message': 'Rename failed on disk (File Busy or Error)'}
                
            except Exception as e:
                print(f"[ORDER] Error en RENAME {old_path} -> {new_path}: {e}")
                import traceback
                traceback.print_exc()
                
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
            
            dir_path = os.path.dirname(path)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)
            
            from ftp.node_transfer import get_node_transfer
            transfer = get_node_transfer()
            
            max_retries = 3
            for attempt in range(max_retries):
                success, actual_path, msg = transfer.request_file_from_node(source_ip, path, path)
                
                if success:
                    metadata = {
                        'size': file_size,
                        'hash': file_hash,
                        'replicas': [source_ip, _cluster_comm_global.local_ip],
                        'operation_id': operation_id,
                        'from_replication': True,
                        'timestamp': time.time(),
                        'source_node': source_ip
                    }
                    
                    state_mgr.append_operation('STOR', path, metadata)
                    
                    if path not in state_mgr.file_map:
                        state_mgr.file_map[path] = {
                            'type': 'file',
                            'size': file_size,
                            'hash': file_hash,
                            'replicas': [source_ip, _cluster_comm_global.local_ip],
                            'mtime': time.time()
                        }
                    else:
                        if _cluster_comm_global.local_ip not in state_mgr.file_map[path].get('replicas', []):
                            state_mgr.file_map[path]['replicas'].append(_cluster_comm_global.local_ip)
                    
                    print(f"[SIDECAR] Réplica exitosa: {path}")
                    
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
    """Este nodo (actuando como LÍDER) recibe una petición de un cliente (via otro nodo)."""
    msg_type = message.get('subtype')
    data = message.get('data', {})
    return process_local_leader_request(msg_type, data)

def handle_mkd_notify(message):
    try:
        path = message.get('path')
        node_ip = message.get('node_ip')
        sm = get_state_manager()
        with sm.lock:
            if path not in sm.file_map:
                sm.file_map[path] = {
                    'type': 'dir',
                    'mtime': time.time(),
                    'replicas': []
                }
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
        'node_ip': _cluster_comm_global.local_ip,
        'partition_epoch': sm.partition_epoch
    }

def handle_sync_commands(message):
    """
    Ejecuta comandos de sincronización enviados por el líder.
    """
    state_mgr = get_state_manager()
    commands = message.get('commands', [])
    phase = message.get('phase', 'unknown')
    print(f"[SIDECAR] Recibidos {len(commands)} comandos de sincronización (fase: {phase})")
    
    results = []
    
    for cmd in commands:
        command = cmd.get('command') or cmd.get('action')
        path = cmd.get('path')
        reason = cmd.get('reason', '')
        
        try:
            if command == 'DELETE_ZOMBIE':
                safe_to_delete = cmd.get('safe_to_delete', False)
                
                print(f"[SYNC] DELETE_ZOMBIE {path} - Razón: {reason} (safe={safe_to_delete})")
                
                if os.path.exists(path):
                    if os.path.isfile(path):
                        if safe_to_delete:
                            try:
                                os.remove(path)
                                print(f"[SYNC] Archivo zombie eliminado: {path}")
                                
                                state_mgr = get_state_manager()
                                state_mgr.append_operation('DELE', path, {
                                    'from_sync': True,
                                    'reason': 'zombie_cleanup',
                                    'original_reason': reason
                                })
                                
                                results.append({
                                    'command': command,
                                    'path': path,
                                    'status': 'deleted'
                                })
                            except Exception as e:
                                print(f"[SYNC] Error eliminando zombie {path}: {e}")
                                results.append({
                                    'command': command,
                                    'path': path,
                                    'status': 'error',
                                    'error': str(e)
                                })
                        else:
                            print(f"[SYNC] ⚠ Zombie {path} NO es seguro de borrar, omitiendo")
                            results.append({
                                'command': command,
                                'path': path,
                                'status': 'skipped',
                                'reason': 'not_safe_to_delete'
                            })
                    elif os.path.isdir(path):
                        if not os.listdir(path):
                            try:
                                os.rmdir(path)
                                print(f"[SYNC] Directorio zombie eliminado: {path}")
                                
                                state_mgr = get_state_manager()
                                state_mgr.append_operation('RMD', path, {
                                    'from_sync': True,
                                    'reason': 'zombie_cleanup'
                                })
                                
                                results.append({
                                    'command': command,
                                    'path': path,
                                    'status': 'deleted'
                                })
                            except Exception as e:
                                print(f"[SYNC] Error eliminando directorio zombie {path}: {e}")
                                results.append({
                                    'command': command,
                                    'path': path,
                                    'status': 'error',
                                    'error': str(e)
                                })
                        else:
                            print(f"[SYNC] ⚠ Directorio zombie {path} no está vacío, omitiendo")
                            results.append({
                                'command': command,
                                'path': path,
                                'status': 'skipped',
                                'reason': 'directory_not_empty'
                            })
                else:
                    print(f"[SYNC] ℹ Archivo zombie {path} ya no existe")
                    results.append({
                        'command': command,
                        'path': path,
                        'status': 'already_gone'
                    })
            
            elif command == 'CREATE_FILE' or command == 'UPDATE_FILE':
                replicas = cmd.get('replicas', [])
                expected_size = cmd.get('size', 0)
                expected_hash = cmd.get('hash', '')
                
                replicas = [ip for ip in replicas if ip != _cluster_comm_global.local_ip]
                
                if not replicas:
                    results.append({'command': command, 'status': 'error', 'error': 'No replicas'})
                    continue
                
                if os.path.exists(path):
                    from ftp.paths import calculate_file_hash
                    existing_hash = calculate_file_hash(path)
                    
                    if existing_hash == expected_hash:
                        print(f"[SYNC] {command} {path} - Ya existe con hash correcto")
                        results.append({
                            'command': command,
                            'path': path,
                            'status': 'already_correct'
                        })
                        continue
                
                print(f"[SYNC] {command} {path} - Descargando (hash={expected_hash[:8] if expected_hash else 'N/A'}...)")
                
                success = False
                for replica_ip in replicas:
                    try:
                        from ftp.node_transfer import get_node_transfer
                        transfer = get_node_transfer()
                        
                        temp_path = f"{path}.tmp.{int(time.time())}"
                        success, actual_path, msg = transfer.request_file_from_node(replica_ip, path, temp_path)
                        
                        if success:
                            from ftp.paths import calculate_file_hash
                            downloaded_hash = calculate_file_hash(temp_path)
                            
                            if expected_hash and downloaded_hash != expected_hash:
                                print(f"[SYNC] Hash incorrecto del archivo descargado")
                                os.remove(temp_path)
                                success = False
                                continue
                            
                            if os.path.exists(path):
                                os.remove(path)
                            os.rename(temp_path, path)
                            print(f"[SYNC] Descargado con hash correcto")
                            break
                            
                    except Exception as e:
                        print(f"[SYNC] Error con réplica {replica_ip}: {e}")
                
                if success:
                    results.append({'command': command, 'path': path, 'status': 'created'})
                else:
                    results.append({'command': command, 'path': path, 'status': 'error'})
                
            elif command == 'CREATE_DIR':
                os.makedirs(path, exist_ok=True)
                print(f"[SYNC] CREATE_DIR {path} - creado/verificado (Razón: {reason})")
                
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

            elif command == 'rename_local_to_version' or command == 'RENAME_LOCAL_TO_VERSION':
                old_path = cmd.get('old_path')
                new_path = cmd.get('new_path')
                expected_hash = cmd.get('expected_hash', '')
                expected_size = cmd.get('expected_size', 0)
                version = cmd.get('version', 0)
                
                print(f"[SYNC] RENAME_LOCAL_TO_VERSION: {old_path} -> {new_path}")
                print(f"[SYNC]   Esperado: hash={expected_hash[:8] if expected_hash else 'N/A'}..., size={expected_size}")
                
                try:
                    if os.path.exists(old_path) and os.path.isfile(old_path):
                        from ftp.paths import calculate_file_hash
                        actual_hash = calculate_file_hash(old_path)
                        actual_size = os.path.getsize(old_path)
                        
                        print(f"[SYNC]   Real: hash={actual_hash[:8] if actual_hash else 'N/A'}..., size={actual_size}")
                        
                        if expected_hash and actual_hash == expected_hash:
                            os.makedirs(os.path.dirname(new_path), exist_ok=True)
                            os.rename(old_path, new_path)
                            print(f"[SYNC] Renombrado exitoso (hash coincide)")
                            
                            state_mgr = get_state_manager()
                            state_mgr.append_operation('RN', new_path, {
                                'old_path': old_path,
                                'from_sync': True,
                                'from_conflict': True,
                                'version': version,
                                'conflict_type': 'file_content_mismatch'
                            })
                            
                            results.append({
                                'command': command,
                                'old_path': old_path,
                                'new_path': new_path,
                                'status': 'renamed',
                                'hash': actual_hash
                            })
                        else:
                            print(f"[SYNC] Hash no coincide")
                            print(f"[SYNC]   Esperado: {expected_hash[:16] if expected_hash else 'N/A'}...")
                            print(f"[SYNC]   Obtenido: {actual_hash[:16] if actual_hash else 'N/A'}...")
                            results.append({
                                'command': command,
                                'status': 'hash_mismatch',
                                'expected_hash': expected_hash[:16] if expected_hash else '',
                                'actual_hash': actual_hash[:16] if actual_hash else ''
                            })
                    else:
                        print(f"[SYNC] ⚠ Archivo origen no existe: {old_path}")
                        if os.path.exists(new_path):
                            print(f"[SYNC] ℹ Archivo versionado ya existe: {new_path}")
                            results.append({'command': command, 'status': 'already_exists'})
                        else:
                            results.append({'command': command, 'status': 'source_missing'})
                            
                except Exception as e:
                    print(f"[SYNC] Error: {e}")
                    import traceback
                    traceback.print_exc()
                    results.append({'command': command, 'status': 'error', 'error': str(e)})

            elif command == 'delete_original' or command == 'DELETE_ORIGINAL':
                # Comando para eliminar archivo original tras crear versiones
                path = cmd.get('path')
                reason = cmd.get('reason', 'unknown')
                
                print(f"[SYNC] DELETE_ORIGINAL: {path} (razón: {reason})")
                
                try:
                    if os.path.exists(path) and os.path.isfile(path):
                        os.remove(path)
                        print(f"[SYNC] Archivo original eliminado: {path}")
                        
                        state_mgr = get_state_manager()
                        state_mgr.append_operation('DELE', path, {
                            'from_sync': True,
                            'reason': reason,
                            'after_conflict_resolution': True
                        })
                        
                        results.append({'command': command, 'path': path, 'status': 'deleted'})
                    else:
                        print(f"[SYNC] ℹ Archivo original ya no existe: {path}")
                        results.append({'command': command, 'path': path, 'status': 'already_gone'})
                except Exception as e:
                    print(f"[SYNC] Error: {e}")
                    results.append({'command': command, 'status': 'error', 'error': str(e)})

            elif command == 'replicate_version' or command == 'REPLICATION':
                path = cmd.get('path')
                source = cmd.get('source_node')
                expected_hash = cmd.get('expected_hash', '')
                
                print(f"[SYNC] REPLICATION: Trayendo {path} desde {source}")
                
                from ftp.node_transfer import get_node_transfer
                transfer = get_node_transfer()
                
                temp_path = f"{path}.tmp.{int(time.time())}"
                success, _, msg = transfer.request_file_from_node(source, path, temp_path)
                
                if success:
                    # Verificar hash
                    from ftp.paths import calculate_file_hash
                    downloaded_hash = calculate_file_hash(temp_path)
                    
                    if expected_hash and downloaded_hash != expected_hash:
                        print(f"[SYNC] Hash no coincide tras replicación")
                        os.remove(temp_path)
                        results.append({'command': command, 'status': 'hash_mismatch'})
                    else:
                        if os.path.exists(path):
                            os.remove(path)
                        os.rename(temp_path, path)
                        print(f"[SYNC] Versión replicada con éxito")
                        
                        state_mgr.append_operation('STOR', path, {
                            'from_sync': True,
                            'source': source,
                            'reason': 'conflict_resolution_replication',
                            'hash': downloaded_hash
                        })
                        
                        results.append({'command': command, 'path': path, 'status': 'replicated'})
                else:
                    print(f"[SYNC] Error en replicación: {msg}")
                    results.append({'command': command, 'status': 'error', 'error': msg})

            else:
                print(f"[SYNC] Comando no soportado: {command}")
                results.append({'command': command, 'status': 'not_implemented'})
                
        except Exception as e:
            print(f"[SYNC] Error ejecutando {command}: {e}")
            import traceback
            traceback.print_exc()
            results.append({'command': command, 'status': 'error', 'error': str(e)})
    
    stats = {
        'total': len(results),
        'success': sum(1 for r in results if r['status'] in ['created', 'renamed', 'already_correct', 'deleted', 'replicated']),
        'errors': sum(1 for r in results if r['status'] == 'error')
    }
    
    print(f"[SYNC] Resumen: {stats}")
    
    return {'status': 'ok', 'results': results, 'stats': stats}

def handle_delta_confirmation(message):
    delta_path = message.get('delta_path')
    node_ip = message.get('node_ip')
    status = message.get('status')
    target_path = message.get('target_path')
    
    print(f"[DELTA-CONFIRM] Confirmación recibida de {node_ip} para {delta_path}")
    
    from ftp.commands import confirm_delta_transfer

    if status == 'success':
        all_done = confirm_delta_transfer(delta_path, node_ip)
        
        if all_done:
            print(f"[DELTA-CONFIRM] Todas las réplicas confirmaron para {delta_path}")
        else:
            print(f"[DELTA-CONFIRM] Aún esperando confirmaciones de otros nodos")
    else:
        print(f"[DELTA-CONFIRM] Nodo {node_ip} reportó fallo en append")
        confirm_delta_transfer(delta_path, node_ip)
    
    return {'status': 'ok'}

def handle_request_disk_scan(message):
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
    global _cluster_comm_global, _bully_instance, _last_known_cluster_size, _nodes_being_integrated
    
    old_ips = set(_cluster_comm_global.cluster_ips)
    new_ips_set = set(new_ips)
    
    added_ips = new_ips_set - old_ips
    removed_ips = old_ips - new_ips_set
    
    # 1. Detección de partición
    partition_detected = False
    if len(new_ips) <= _last_known_cluster_size * 0.67:
        print(f"[SIDECAR] PARTICIÓN DETECTADA (pérdida de nodos): {len(new_ips)} nodos (antes: {_last_known_cluster_size})")
        partition_detected = True
    elif _last_known_cluster_size <= 4 and len(removed_ips) >= 2:
        print(f"[SIDECAR] PARTICIÓN DETECTADA (cluster pequeño): perdidos {len(removed_ips)} de {_last_known_cluster_size} nodos")
        partition_detected = True
    
    if partition_detected:
        state_mgr = get_state_manager()
        state_mgr.increment_partition_epoch()
    
    _last_known_cluster_size = len(new_ips)
    _cluster_comm_global.update_cluster_ips(new_ips)
    _bully_instance.update_nodes(new_ips)
    
    # 2. Gestión de cambios si soy el líder
    if _bully_instance and _bully_instance.am_i_leader():
        # Procesar salidas inmediatamente
        for failed_ip in removed_ips:
            print(f"[SIDECAR] Nodo {failed_ip} ha salido, gestionando fallo...")
            handle_node_failure(failed_ip)
        
        # Procesar entradas con bloqueo para evitar duplicados
        with _integration_lock:
            actually_new = [ip for ip in added_ips if ip not in _nodes_being_integrated]
            if actually_new:
                for ip in actually_new:
                    _nodes_being_integrated.add(ip)
                
                print(f"[SIDECAR] Iniciando reintegración para: {actually_new}")
                threading.Thread(
                    target=perform_bulk_reintegration, 
                    args=(actually_new,), 
                    daemon=True
                ).start()

def perform_bulk_reintegration(new_nodes):
    state_mgr = get_state_manager()
    cc = _cluster_comm_global
    combined_data_for_merge = []

    print(f"[SIDECAR-BULK] Iniciando reintegración de {len(new_nodes)} nodos")

    # Incluir TODOS los nodos del cluster, no solo los nuevos
    all_nodes = cc.cluster_ips.copy()
    existing_nodes = [ip for ip in all_nodes if ip not in new_nodes and ip != cc.local_ip]
    
    print(f"[SIDECAR-BULK] Nodos nuevos: {new_nodes}")
    print(f"[SIDECAR-BULK] Nodos existentes: {existing_nodes}")
    print(f"[SIDECAR-BULK] Total a procesar: {len(all_nodes)} nodos")

    # 1. Recolección CON disk_scan - TODOS los nodos (nuevos + existentes)
    all_remote_nodes = new_nodes + existing_nodes
    
    for ip in all_remote_nodes:
        print(f"[SIDECAR-BULK] Solicitando datos de {ip}...")
        
        resp_logs = cc.send_message(ip, {'type': 'REQUEST_LOGS'}, expect_response=True)
        
        # Siempre solicitar disk_scan
        from ftp.paths import SERVER_ROOT
        resp_scan = cc.send_message(ip, {
            'type': 'REQUEST_DISK_SCAN',
            'root_path': os.path.join(SERVER_ROOT, 'root')
        }, expect_response=True)
        
        node_log = resp_logs.get('log', []) if resp_logs and resp_logs.get('status') == 'ok' else []
        node_scan = resp_scan.get('disk_scan', {}) if resp_scan and resp_scan.get('status') == 'ok' else {}
        
        print(f"[SIDECAR-BULK] {ip}: {len(node_log)} ops, {len(node_scan)} archivos en disco")
        
        combined_data_for_merge.append({
            'node_ip': ip,
            'log': node_log,
            'scan': node_scan
        })

    # Añadir datos del líder CON disk_scan
    from ftp.paths import SERVER_ROOT
    leader_scan = state_mgr.scan_local_filesystem(os.path.join(SERVER_ROOT, 'root'))
    
    combined_data_for_merge.append({
        'node_ip': cc.local_ip,
        'log': state_mgr.get_full_log_as_dict(),
        'scan': leader_scan
    })
    
    print(f"[SIDECAR-BULK] Líder: {len(leader_scan)} archivos en disco")

    # 2. Merge con detección de conflictos
    print(f"[SIDECAR-BULK] Fusionando datos de {len(combined_data_for_merge)} nodos totales")
    _, conflict_actions = state_mgr.merge_external_logs(combined_data_for_merge)

    # 3. Ejecución de acciones por fases (prioridad)
    if conflict_actions:
        print(f"[SIDECAR-BULK] Ejecutando {len(conflict_actions)} acciones en 3 fases")
        
        # Mostrar distribución de acciones por nodo
        actions_by_node = defaultdict(int)
        for action in conflict_actions:
            node = action.get('node') or action.get('target_node')
            if node:
                actions_by_node[node] += 1
        print(f"[SIDECAR-BULK] Distribución de acciones por nodo: {dict(actions_by_node)}")
        
        # FASE 1: Renombrar (priority=1)
        phase1_actions = [a for a in conflict_actions if a.get('priority') == 1]
        if phase1_actions:
            print(f"[SIDECAR-BULK] FASE 1: Renombrando {len(phase1_actions)} archivos...")
            execute_sync_actions(phase1_actions, 'rename')
            time.sleep(2)
        
        # FASE 2: Replicar (priority=2)
        phase2_actions = [a for a in conflict_actions if a.get('priority') == 2]
        if phase2_actions:
            print(f"[SIDECAR-BULK] FASE 2: Replicando {len(phase2_actions)} versiones...")
            execute_sync_actions(phase2_actions, 'replicate')
            time.sleep(3)
        
        # FASE 3: Borrar originales (priority=3)
        phase3_actions = [a for a in conflict_actions if a.get('priority') == 3]
        if phase3_actions:
            print(f"[SIDECAR-BULK] FASE 3: Borrando {len(phase3_actions)} archivos originales...")
            execute_sync_actions(phase3_actions, 'delete_original')
            time.sleep(1)

    # 4. Finalización
    ops = get_leader_operations()
    if ops:
        for ip in new_nodes:
            ops.handle_node_join(ip)
    
    with _integration_lock:
        all_nodes = cc.cluster_ips.copy()
        existing_nodes = [ip for ip in all_nodes if ip not in new_nodes and ip != cc.local_ip]

        print(f"[SIDECAR-BULK] Nodos nuevos: {new_nodes}")
        print(f"[SIDECAR-BULK] Nodos existentes: {existing_nodes}")

        all_remote_nodes = new_nodes + existing_nodes
        for ip in all_remote_nodes:
            _nodes_being_integrated.discard(ip)
    
    print(f"[SIDECAR-BULK] Reintegración completada")

def execute_sync_actions(actions, phase_name):
    """Ejecuta acciones de sincronización agrupadas por nodo"""
    cc = _cluster_comm_global
    local_ip = cc.local_ip
    
    # Agrupar por nodo
    actions_by_node = defaultdict(list)
    for action in actions:
        target = action.get('node') or action.get('target_node')
        if target:
            actions_by_node[target].append(action)
    
    # Enviar a cada nodo
    for node_ip, node_actions in actions_by_node.items():
        print(f"[SIDECAR-BULK] Enviando {len(node_actions)} acciones ({phase_name}) a {node_ip}")
        
        if node_ip == local_ip:
            # Ejecutar localmente
            handle_sync_commands({
                'type': 'SYNC_COMMANDS',
                'commands': node_actions,
                'phase': phase_name
            })
        else:
            # Enviar a nodo remoto
            try:
                cc.send_message(node_ip, {
                    'type': 'SYNC_COMMANDS',
                    'commands': node_actions,
                    'phase': phase_name
                }, expect_response=False)
            except Exception as e:
                print(f"[SIDECAR-BULK] Error enviando a {node_ip}: {e}")

def start_replication_checker():
        """Verifica periódicamente el estado de replicación."""
        while True:
            time.sleep(60)
            
            if _bully_instance and _bully_instance.am_i_leader():
                ops = get_leader_operations()
                if ops:
                    ops.check_replication_status()

# --- FUNCIONES COMPLEMENTARIAS ---

def handle_node_failure(failed_ip: str):
    print(f"[SIDECAR] Manejando falla del nodo: {failed_ip}")
    
    bully = get_global_bully()
    cluster_comm = get_global_cluster_comm()
    
    if not bully or not cluster_comm:
        print(f"[SIDECAR] No se puede manejar falla del nodo: cluster no inicializado")
        return
    
    if bully.am_i_leader():
        ops = get_leader_operations()
        if ops:
            ops.handle_node_failure(failed_ip)
    else:
        print(f"[SIDECAR] No soy líder, no manejo fallas de nodos")

def _handle_file_rename(old_path, new_path, operation_id, message):
    """
    Maneja el renombre de archivo.
    IMPORTANTE: Registra como DELE + STOR para mantener independencia de archivos
    """
    if os.path.exists(old_path):
        if os.path.exists(new_path):
            print(f"[ORDER] ERROR: El archivo destino ya existe: {new_path}")
            return False
        
        os.makedirs(os.path.dirname(new_path), exist_ok=True)
        
        try:
            # Calcular hash antes de mover (para el STOR)
            from ftp.paths import calculate_file_hash
            file_hash = calculate_file_hash(old_path)
            file_size = os.path.getsize(old_path)
            
            # Realizar el rename físico
            os.rename(old_path, new_path)
            print(f"[ORDER] Archivo renombrado: {old_path} -> {new_path}")
            
            # Registrar como DELE del original + STOR del nuevo
            state_mgr = get_state_manager()
            
            # 1. Registrar eliminación del archivo original
            state_mgr.append_operation('DELE', old_path, {
                'operation_id': operation_id,
                'executed_by': _cluster_comm_global.local_ip,
                'from_rename': True,
                'renamed_to': new_path
            })
            
            # 2. Registrar creación del archivo nuevo
            state_mgr.append_operation('STOR', new_path, {
                'size': file_size,
                'hash': file_hash,
                'operation_id': operation_id,
                'executed_by': _cluster_comm_global.local_ip,
                'from_rename': True,
                'renamed_from': old_path,
                'replicas': [_cluster_comm_global.local_ip]
            })
            
            return True
            
        except OSError as e:
            if e.errno == 26 or "Text file busy" in str(e):
                print(f"[ORDER] ERROR: El archivo {old_path} está siendo usado (ETXTBSY).")
            else:
                print(f"[ORDER] Error de OS al renombrar: {e}")
            
            return False

    else:
        state_mgr = get_state_manager()
        file_info = state_mgr.get_file_info(old_path)
        
        if file_info and _cluster_comm_global.local_ip in file_info.get('replicas', []):
            print(f"[ORDER] INCONSISTENCIA: Somos réplica de {old_path} pero no lo tenemos")
            
            replicas = file_info.get('replicas', [])
            source_replicas = [ip for ip in replicas if ip != _cluster_comm_global.local_ip]
            
            if source_replicas:
                from ftp.node_transfer import get_node_transfer
                transfer = get_node_transfer()
                
                for source_ip in source_replicas:
                    print(f"[ORDER] Intentando descargar {old_path} desde {source_ip}")
                    success, _, _ = transfer.request_file_from_node(source_ip, old_path, old_path)
                    
                    if success:
                        try:
                            # Calcular hash antes de mover
                            from ftp.paths import calculate_file_hash
                            file_hash = calculate_file_hash(old_path)
                            file_size = os.path.getsize(old_path)
                            
                            os.rename(old_path, new_path)
                            print(f"[ORDER] Archivo renombrado: {old_path} -> {new_path}")
                            
                            # Registrar como DELE + STOR
                            state_mgr.append_operation('DELE', old_path, {
                                'operation_id': operation_id,
                                'executed_by': _cluster_comm_global.local_ip,
                                'from_rename': True
                            })
                            
                            state_mgr.append_operation('STOR', new_path, {
                                'size': file_size,
                                'hash': file_hash,
                                'operation_id': operation_id,
                                'executed_by': _cluster_comm_global.local_ip,
                                'from_rename': True,
                                'replicas': [_cluster_comm_global.local_ip]
                            })
                            
                            return True
                        except OSError as e:
                            if e.errno == 26 or "Text file busy" in str(e):
                                print(f"[ORDER] ERROR: El archivo {old_path} está siendo usado (ETXTBSY).")
                            else:
                                print(f"[ORDER] Error de OS al renombrar: {e}")
                            
                            return False
        
        print(f"[ORDER] No somos réplica de {old_path}, solo actualizamos estado")
        return True

def _handle_directory_rename(old_path, new_path, operation_id):
    print(f"[ORDER] Renombrando directorio: {old_path} -> {new_path}")
    
    if os.path.exists(old_path):
        os.makedirs(os.path.dirname(new_path), exist_ok=True)
        
        if os.path.exists(new_path):
            print(f"[ORDER] ERROR: El directorio destino ya existe: {new_path}")
            return False
        
        try:
            os.rename(old_path, new_path)
            print(f"[ORDER] Directorio renombrado: {old_path} -> {new_path}")
            
            if os.path.exists(old_path):
                print(f"[ORDER] ADVERTENCIA: Directorio antiguo aún existe después de rename")
                try:
                    if os.path.isdir(old_path) and not os.listdir(old_path):
                        os.rmdir(old_path)
                except:
                    pass
            
            return True
        except OSError as e:
            print(f"[ORDER] Error de OS al renombrar directorio: {e}")
            return False
    else:
        print(f"[ORDER] Directorio {old_path} no existe localmente, creando {new_path}")
        
        try:
            os.makedirs(new_path, exist_ok=True)
            return True
        except Exception as e:
            print(f"[ORDER] Error creando directorio: {e}")
            return False

def create_and_log_dirs(final_path, state_mgr, local_ip, operation_id):
    try:
        final_path = os.path.abspath(final_path)
        
        if not os.path.exists(final_path):
            os.makedirs(final_path, exist_ok=True)
            print(f"[ORDER] Directorio creado: {final_path}")
            
            state_mgr.append_operation(
                'MKD',
                final_path,
                {
                    'operation_id': operation_id,
                    'executed_by': local_ip,
                    'from_order': True
                }
            )
        else:
            print(f"[ORDER] Directorio ya existía: {final_path}")
            state_mgr.append_operation(
                'MKD',
                final_path,
                {
                    'operation_id': operation_id,
                    'executed_by': local_ip,
                    'already_exists': True
                }
            )
            
    except Exception as e:
        print(f"[ORDER] Error creando directorio {final_path}: {e}")

def handle_node_join(new_ip):
    """
    Maneja la entrada de un nodo nuevo o reintegrado.
    Sincroniza archivos faltantes Y limpia zombies.
    """
    bully = get_global_bully()
    if not bully or not bully.am_i_leader():
        return

    print(f"[SIDECAR] Nodo {new_ip} unido. Iniciando sincronización completa...")
    cc = get_global_cluster_comm()
    
    # 1. Solicitar logs del nodo
    resp_logs = cc.send_message(new_ip, {'type': 'REQUEST_LOGS'}, expect_response=True)
    
    if not resp_logs or resp_logs.get('status') != 'ok':
        print(f"[SIDECAR] No se pudieron obtener logs de {new_ip}")
        return
    
    node_logs = resp_logs.get('log', [])
    
    # 2. Solicitar escaneo del disco fí­sico
    print(f"[SIDECAR] Solicitando escaneo de disco a {new_ip}...")
    resp_scan = cc.send_message(new_ip, {
        'type': 'REQUEST_DISK_SCAN',
        'root_path': os.environ.get('SERVER_ROOT', '/app/server/data')
    }, expect_response=True)
    
    disk_scan = None
    if resp_scan and resp_scan.get('status') == 'ok':
        disk_scan = resp_scan.get('disk_scan', {})
        print(f"[SIDECAR] Escaneo recibido: {len(disk_scan)} elementos en disco")
    else:
        print(f"[SIDECAR] No se pudo escanear disco, usando solo logs")
    
    # 3. Analizar inconsistencias (faltantes, desactualizados Y zombies)
    sm = get_state_manager()
    analysis = sm.analyze_node_state(node_logs, new_ip, disk_scan)
    
    print(f"[SIDECAR] Análisis de nodo {new_ip}:")
    print(f"  - Total inconsistencias: {len(analysis['inconsistencies'])}")
    print(f"  - Zombies: {analysis.get('node_state_summary', {}).get('zombie_count', 0)}")
    print(f"  - Faltantes: {analysis.get('node_state_summary', {}).get('missing_count', 0)}")
    print(f"  - Desactualizados: {analysis.get('node_state_summary', {}).get('outdated_count', 0)}")
    
    # 4. SINCRONIZACIÓN (replicación + limpieza)
    if analysis['has_inconsistencies']:

        # separar pero NO limpiar primero
        replications = [
            cmd for cmd in analysis['inconsistencies']
            if cmd.get('command') in ['CREATE_FILE', 'UPDATE_FILE', 'CREATE_DIR']
        ]

        zombies = [
            cmd for cmd in analysis['inconsistencies']
            if cmd.get('command') == 'DELETE_ZOMBIE'
        ]

        # Fase 1: REPLICAR PRIMERO
        if replications:
            print(f"[SIDECAR] Fase 1: Replicando {len(replications)} archivos faltantes...")
            cc.send_message(new_ip, {
                'type': 'SYNC_COMMANDS',
                'commands': replications,
                'phase': 'replication'
            }, expect_response=False)

            # Esperar a que se escriba el estado
            time.sleep(2)

        # Filtrar zombies seguros
        safe_zombies = [
            z for z in zombies
            if z.get('safe_to_delete', False)
        ]

        # Fase 2: limpieza SOLO si es seguro
        if safe_zombies:
            print(f"[SIDECAR] Fase 2: Limpiando {len(safe_zombies)} archivos zombie seguros...")
            cc.send_message(new_ip, {
                'type': 'SYNC_COMMANDS',
                'commands': safe_zombies,
                'phase': 'zombie_cleanup'
            }, expect_response=False)
        else:
            if zombies:
                print(f"[SIDECAR] Zombies detectados pero diferidos (no seguros de borrar)")

    else:
        print(f"[SIDECAR] Nodo {new_ip} está completamente sincronizado.")
    
    # 5. ASIGNAR NUEVAS RÉPLICAS (si es necesario)
    ops = get_leader_operations()
    if ops:
        print(f"[SIDECAR] Verificando necesidad de réplicas para {new_ip}...")
        ops.handle_node_join(new_ip)
    
    print(f"[SIDECAR] Sincronización de {new_ip} finalizada")

# --- CONSULTAS GLOBALES ---

def get_global_cluster_comm():
    """Obtiene la instancia global de cluster_comm"""
    return _cluster_comm_global

def get_global_bully():
    """Para que otros módulos consulten quién es el líder"""
    return _bully_instance