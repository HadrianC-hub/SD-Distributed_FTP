import os
import time
import random
import hashlib
import threading
from typing import List, Dict, Tuple
from ftp.state_manager import get_state_manager, LockManager

class LeaderOperations:
    def __init__(self, cluster_comm, bully):
        self.state_mgr = get_state_manager()
        self.cluster_comm = cluster_comm
        self.bully = bully
        self.lock_mgr = LockManager()
        
    # --- FUNCIONES DE MANEJO DEL LIDER ---

    def handle_mkd_request(self, requester_ip: str, path: str, session_user: str = None) -> Dict:
        if self.state_mgr.get_file_info(path):
            return {'status': 'error', 'code': '550', 'message': 'Directory already exists'}
        
        # Calcular réplicas (mismo que calculate_replica_nodes o lógica simple)
        all_ips = self.cluster_comm.cluster_ips
        replica_nodes = all_ips if len(all_ips) <= 3 else self.calculate_replica_nodes(path)
        if requester_ip not in replica_nodes and requester_ip in all_ips:
            replica_nodes[0] = requester_ip # Preferir al requester

        operation_id = f"mkd_{int(time.time())}_{random.randint(1000,9999)}"
        self.state_mgr.file_map[path] = {
            'type': 'dir', 'mtime': time.time(),
            'created_by': session_user, 'replicas': replica_nodes
        }
        self.state_mgr.append_operation('MKD', path, {'nodes': replica_nodes, 'requester': requester_ip})
        
        self._send_operation_to_nodes('CREATE_DIR', path, replica_nodes, operation_id)
        return {'status': 'ok', 'command': 'CREATE_DIR', 'path': path, 'nodes': replica_nodes, 'operation_id': operation_id}
   
    def handle_rmd_request(self, requester_ip: str, path: str, session_user: str = None) -> Dict:
        """
        Maneja solicitud de eliminación de directorio.
        PROTECCIÓN: Verifica recursivamente que no haya locks en archivos dentro.
        """
        print(f"[LEADER] RMD request from {requester_ip} for {path}")
        
        dir_info = self.state_mgr.get_file_info(path)
        if not dir_info or dir_info.get('type') != 'dir':
            return {'status': 'error', 'code': '550', 'message': 'Directory not found'}
        
        # VERIFICACIÓN RECURSIVA CRÍTICA: Comprobar locks en el directorio y contenido
        has_locks, locked_paths = self._check_locks_recursive(path)
        
        if has_locks:
            print(f"[LEADER] Cannot delete directory {path}: contains locked files")
            print(f"[LEADER] Locked paths: {locked_paths}")
            
            return {
                'status': 'error',
                'code': '450',
                'message': f'Directory contains files currently locked by other operations. '
                          f'{len(locked_paths)} file(s) locked. Please try again later.'
            }
        
        # Si no hay locks, proceder con la eliminación
        all_nodes = self.cluster_comm.cluster_ips
        operation_id = f"rmd_{int(time.time())}"
        
        # Eliminar del mapa global
        if path in self.state_mgr.file_map:
            del self.state_mgr.file_map[path]
            
        self.state_mgr.append_operation('RMD', path, {'requester': requester_ip})
        self._send_operation_to_nodes('DELETE_DIR', path, all_nodes, operation_id)
        
        print(f"[LEADER] RMD approved for {path}")
        return {'status': 'ok', 'command': 'DELETE_DIR', 'path': path, 'nodes': all_nodes}

    def handle_dele_request(self, requester_ip: str, path: str, session_user: str = None) -> Dict:
        """
        Maneja solicitud de eliminación de archivo.
        PROTECCIÓN: Verifica que no haya locks activos antes de eliminar.
        """
        print(f"[LEADER] DELE request from {requester_ip} for {path}")
        
        file_info = self.state_mgr.get_file_info(path)
        if not file_info:
            return {'status': 'error', 'code': '550', 'message': 'File not found'}
        
        # VERIFICACIÓN CRÍTICA: Comprobar si hay lock activo
        with self.lock_mgr.lock:
            if path in self.lock_mgr.locks:
                lock_holder = self.lock_mgr.locks[path]['holder']
                lock_time = self.lock_mgr.locks[path].get('ts', 0)
                lock_age = time.time() - lock_time
                
                print(f"[LEADER] Cannot delete {path}: locked by {lock_holder} ({lock_age:.1f}s ago)")
                
                return {
                    'status': 'error',
                    'code': '450',
                    'message': f'File is currently locked by another operation. Please try again later.'
                }
        
        # Si no hay lock, proceder con la eliminación
        replica_nodes = file_info.get('replicas', [])
        operation_id = f"dele_{int(time.time())}"
        
        if path in self.state_mgr.file_map:
            del self.state_mgr.file_map[path]
            
        self.state_mgr.append_operation('DELE', path, {'requester': requester_ip})
        self._send_operation_to_nodes('DELETE_FILE', path, replica_nodes, operation_id)
        
        print(f"[LEADER] DELE approved for {path}")
        return {'status': 'ok', 'command': 'DELETE_FILE', 'path': path}

    def handle_retr_request(self, requester_ip: str, path: str, session_user: str = None) -> Dict:
        """
        Maneja solicitud de descarga (RETR).
        1. Verifica existencia.
        2. Adquiere lock de lectura (compartido).
        3. Decide el mejor nodo fuente.
        """
        # 1. Verificar existencia
        file_info = self.state_mgr.get_file_info(path)
        if not file_info or file_info.get('type') != 'file':
            return {'status': 'error', 'code': '550', 'message': 'File not found'}
        
        # 2. Adquirir Lock de Lectura (Permite múltiples lectores, bloquea escritores/borrado)
        lock_acquired, lock_id = self.acquire_file_lock(path, requester_ip, lock_type='read')
        
        if not lock_acquired:
            holder = self.lock_mgr.locks.get(path, {}).get('holder', 'unknown')
            # Si es lock de lectura y el holder es otro lector, idealmente permitiríamos,
            # pero para tu código simple, si está bloqueado, rebotamos.
            return {'status': 'error', 'code': '450', 'message': f'File is busy/locked by {holder}'}
        
        # 3. Seleccionar fuente
        replicas = file_info.get('replicas', [])
        source = None
        
        # Si el solicitante tiene el archivo, él es la fuente
        if requester_ip in replicas:
            source = requester_ip
        else:
            # Si no, elegir una réplica aleatoria (balanceo de carga simple)
            if replicas:
                source = random.choice(replicas)
        
        if not source:
             self.release_file_lock(path, requester_ip)
             return {'status': 'error', 'message': 'No replicas available'}

        operation_id = f"retr_{int(time.time())}_{random.randint(1000,9999)}"
        
        return {
            'status': 'ok', 
            'source': source, 
            'replicas': replicas, 
            'operation_id': operation_id,
            'lock_id': lock_id,
            'size': file_info.get('size', 0)
        }
   
    def handle_rnfr_request(self, requester_ip: str, path: str, session_user: str = None) -> Dict:
        """Maneja solicitud de RNFR (preparar renombrado con lock)."""
        print(f"[LEADER] RNFR request from {requester_ip} for {path}")
        
        # Verificar que el archivo/directorio exista en el estado global
        file_info = self.state_mgr.get_file_info(path)
        if not file_info:
            # También verificar si es un directorio que contiene archivos
            # Buscar cualquier entrada que comience con este path
            is_dir_with_files = any(
                fpath.startswith(path.rstrip('/') + '/') 
                for fpath in self.state_mgr.file_map.keys()
            )
            
            if not is_dir_with_files:
                return {'status': 'error', 'message': 'File or directory not found in global state'}
            
            # Si es un directorio vacío, crear entrada temporal
            file_info = {
                'type': 'dir',
                'replicas': [],
                'mtime': time.time()
            }
        
        # Adquirir lock de escritura
        lock_acquired, lock_id = self.acquire_file_lock(path, requester_ip, lock_type='write')
        if not lock_acquired:
            current_holder = self.lock_mgr.locks.get(path, {}).get('holder', 'unknown')
            return {'status': 'error', 'message': f'File is locked by {current_holder}'}
        
        return {
            'status': 'ok', 
            'lock_id': lock_id,
            'type': file_info.get('type', 'file'),
            'replicas': file_info.get('replicas', [])
        }
    
    def handle_rnto_request(self, requester_ip: str, old_path: str, new_path: str, 
                            lock_id: str, session_user: str = None) -> Dict:
        """Maneja solicitud de RNTO (ejecutar renombrado coordinado)."""
        print(f"[LEADER] RNTO request from {requester_ip}: {old_path} -> {new_path}")
        
        # Verificar que el lock sea válido
        current_lock = self.lock_mgr.locks.get(old_path)
        if not current_lock or current_lock.get('holder') != requester_ip:
            return {'status': 'error', 'message': 'Invalid or expired lock'}
        
        # Verificar que el archivo/directorio aún exista
        file_info = self.state_mgr.get_file_info(old_path)
        if not file_info:
            return {'status': 'error', 'message': 'Source not found'}
        
        # Verificar que el nuevo nombre no exista
        if self.state_mgr.get_file_info(new_path):
            return {'status': 'error', 'message': 'Target already exists'}
        
        # Obtener lista de réplicas
        item_type = file_info.get('type', 'file')
        
        # PARA DIRECTORIOS: enviar a TODOS los nodos del cluster
        if item_type == 'dir':
            # Los directorios deben renombrarse en todos los nodos
            replica_nodes = self.cluster_comm.cluster_ips.copy()
            
            # También necesitamos obtener todos los archivos dentro del directorio
            old_prefix = old_path.rstrip('/') + '/'
            
            # Identificar todos los archivos que están dentro de este directorio
            files_in_dir = []
            with self.state_mgr.lock:
                for path, info in self.state_mgr.file_map.items():
                    if path.startswith(old_prefix):
                        files_in_dir.append((path, info))
        else:
            # Para archivos, usar las réplicas registradas
            replica_nodes = file_info.get('replicas', [])
            files_in_dir = []
        
        # Crear operación ID
        operation_id = f"rn_{int(time.time())}_{random.randint(1000, 9999)}"
        
        # Actualizar el estado global
        with self.state_mgr.lock:
            # Si es un directorio, actualizar todas las rutas que empiecen con old_path
            if item_type == 'dir':
                # Cambiar la entrada principal del directorio
                self.state_mgr.file_map[new_path] = file_info.copy()
                del self.state_mgr.file_map[old_path]
                
                # Cambiar todas las rutas que empiecen con old_path/
                old_prefix = old_path.rstrip('/') + '/'
                new_prefix = new_path.rstrip('/') + '/'
                
                keys_to_update = [k for k in self.state_mgr.file_map.keys() 
                                if k.startswith(old_prefix)]
                
                for old_key in keys_to_update:
                    # Calcular nueva ruta
                    relative_part = old_key[len(old_prefix):]
                    new_key = new_prefix + relative_part
                    
                    # Mover la entrada
                    self.state_mgr.file_map[new_key] = self.state_mgr.file_map[old_key]
                    del self.state_mgr.file_map[old_key]
                    
                    print(f"[LEADER] Actualizada ruta en estado global: {old_key} -> {new_key}")
            else:
                # Para archivos, simplemente mover la entrada
                self.state_mgr.file_map[new_path] = file_info
                del self.state_mgr.file_map[old_path]
        
        # Registrar operación en el log
        metadata = {
            'old_path': old_path,
            'new_path': new_path,
            'requester': requester_ip,
            'replicas': replica_nodes,
            'lock_id': lock_id,
            'operation_id': operation_id,
            'type': item_type,
            'is_directory': item_type == 'dir',
            'files_in_dir_count': len(files_in_dir) if item_type == 'dir' else 0
        }
        self.state_mgr.append_operation('RN', new_path, metadata)
        
        # Liberar el lock
        self.release_file_lock(old_path, requester_ip)
        
        # Enviar orden de renombrar a los nodos correspondientes
        self._send_rename_order_to_nodes(old_path, new_path, replica_nodes, operation_id, item_type, files_in_dir)
        
        return {
            'status': 'ok',
            'command': 'RENAME',
            'operation_id': operation_id,
            'replicas': replica_nodes,
            'is_directory': item_type == 'dir'
        }
    
    def handle_stor_request(self, requester_ip: str, path: str, size: int = 0, hash: str = None, session_user: str = None, append: bool = False) -> Dict:
        all_nodes = self.cluster_comm.cluster_ips
        replica_nodes = self.calculate_replica_nodes(path)
        
        # Asegurar requester
        if requester_ip not in replica_nodes and requester_ip in all_nodes:
            if len(replica_nodes) >= 3: replica_nodes[-1] = requester_ip
            else: replica_nodes.append(requester_ip)
            
        operation_id = f"stor_{int(time.time())}_{random.randint(1000,9999)}"
        
        self.state_mgr.file_map[path] = {
            'type': 'file', 'size': size, 'hash': hash, 'mtime': time.time(),
            'replicas': replica_nodes, 'created_by': session_user
        }
        self.state_mgr.append_operation('STOR', path, {'size': size, 'hash': hash, 'replicas': replica_nodes})
        
        # Replicar a otros
        for node_ip in replica_nodes:
            if node_ip != requester_ip:
                self._send_replicate_order(node_ip, path, requester_ip, size, hash, operation_id)
                
        return {'status': 'ok', 'command': 'STOR', 'path': path, 'replicas': replica_nodes, 'operation_id': operation_id}

    def handle_appe_request(self, requester_ip: str, path: str, delta_size: int, session_user: str = None) -> Dict:
        """
        Maneja solicitud APPE. Actualiza metadatos y devuelve réplicas.
        """
        print(f"[LEADER] APPE request from {requester_ip} for {path} (+{delta_size} bytes)")
        
        # 1. Verificar existencia
        file_info = self.state_mgr.get_file_info(path)
        if not file_info or file_info.get('type') != 'file':
            return {'status': 'error', 'code': '550', 'message': 'File not found'}
        
        # 2. Adquirir Lock de escritura (importante para evitar condiciones de carrera)
        lock_acquired, lock_id = self.acquire_file_lock(path, requester_ip, 'write')
        if not lock_acquired:
            return {'status': 'error', 'code': '550', 'message': f'File is locked: {lock_id}'}
        
        # 3. Actualizar metadatos globales
        new_size = file_info.get('size', 0) + delta_size
        file_info['size'] = new_size
        file_info['mtime'] = time.time()
        
        # 4. Registrar operación
        operation_id = f"appe_{int(time.time())}_{random.randint(1000, 9999)}"
        metadata = {
            'delta_size': delta_size,
            'requester': requester_ip,
            'replicas': file_info['replicas'],
            'operation_id': operation_id
        }
        self.state_mgr.append_operation('APPE', path, metadata)
        
        return {
            'status': 'ok',
            'command': 'APPE',
            'path': path,
            'replicas': file_info['replicas'],
            'operation_id': operation_id,
            'lock_id': lock_id
        }

    def handle_list_request(self, requester_ip, abs_path, user_root, session_user=None):
        dir_info = self.state_mgr.get_file_info(abs_path)
        if not dir_info or dir_info.get('type') != 'dir': return {'status': 'error', 'message': 'Not a dir'}
        
        items = []
        prefix = abs_path.rstrip('/') + '/'
        for fpath, info in self.state_mgr.file_map.items():
            if fpath == abs_path: continue
            if fpath.startswith(prefix):
                rel = fpath[len(prefix):]
                if '/' not in rel:
                    items.append({'name': rel, 'type': info.get('type'), 'size': info.get('size', 0), 'mtime': info.get('mtime', 0)})
        return {'status': 'ok', 'items': items}
   
    def handle_nlst_request(self, requester_ip, abs_path, user_root, session_user=None):
        res = self.handle_list_request(requester_ip, abs_path, user_root, session_user)
        if res['status'] == 'ok':
            res['names'] = [x['name'] for x in res['items']]
        return res
  
    def handle_cwd_request(self, requester_ip, abs_path, user_root, session_user=None):
        if abs_path == user_root and not self.state_mgr.get_file_info(user_root):
            self.state_mgr.file_map[user_root] = {'type': 'dir', 'mtime': time.time(), 'replicas': self.cluster_comm.cluster_ips}
            
        info = self.state_mgr.get_file_info(abs_path)
        if info and info.get('type') == 'dir': return {'status': 'ok', 'abs_path': abs_path}
        return {'status': 'error', 'message': 'Directory not found'}

    def handle_pwd_request(self, requester_ip: str, abs_path: str, user_root: str, session_user: str = None) -> Dict:
        """
        Maneja solicitud de PWD.
        """
        print(f"[LEADER] PWD request from {requester_ip} for abs_path: {abs_path}, user_root: {user_root}")
        
        # Asegurar que el directorio raíz del usuario exista
        if abs_path == user_root:
            # Crear entrada para el directorio raíz del usuario si no existe
            if not self.state_mgr.get_file_info(user_root):
                self.state_mgr.file_map[user_root] = {
                    'type': 'dir',
                    'mtime': time.time(),
                    'created_by': 'system',
                    'replicas': self.cluster_comm.cluster_ips
                }
                print(f"[LEADER] Directorio raíz de usuario creado: {user_root}")
        
        # Validar que el directorio existe en el esquema global
        dir_info = self.state_mgr.get_file_info(abs_path)
        if not dir_info or dir_info.get('type') != 'dir':
            return {'status': 'error', 'code': '550', 'message': 'Directory not found'}
        
        return {
            'status': 'ok',
            'abs_path': abs_path,
            'user_root': user_root,
            'operation_id': f"pwd_{int(time.time())}"
        }

    def handle_cdup_request(self, requester_ip, abs_path, user_root, session_user=None):
        if abs_path == user_root: return {'status': 'ok', 'abs_path': abs_path}
        parent = os.path.dirname(abs_path)
        if not parent.startswith(user_root): parent = user_root
        return self.handle_cwd_request(requester_ip, parent, user_root, session_user)

    def handle_release_lock_request(self, requester_ip: str, path: str, session_user: str = None) -> Dict:
        """
        Maneja la solicitud de liberación de un lock por parte de un nodo.
        El nodo que solicita la liberación debe ser el que adquirió el lock.
        """
        print(f"[LEADER] Liberando lock en {path} solicitado por {requester_ip}")
        
        # Llama al método de LockManager para liberar el lock.
        # Solo el nodo que lo tiene puede liberarlo (o el líder, pero aquí el nodo lo pide).
        if self.lock_mgr.release_lock(path, requester_ip):
            print(f"[LEADER] Lock liberado para {path} por {requester_ip}")
            return {'status': 'ok', 'message': f'Lock released for {path}'}
        else:
            # El lock no se pudo liberar (no existe o no lo tiene este requester)
            current_holder = self.lock_mgr.locks.get(path, {}).get('holder', 'N/A')
            print(f"[LEADER] Fallo en liberación de lock para {path}. Holder actual: {current_holder}")
            return {'status': 'error', 'message': f'Failed to release lock. Current holder: {current_holder}'}

    def handle_completion(self, op_id, success, msg):
        print(f"[LEADER] Op {op_id} finished: {success}")
        return {'status': 'ok'}
  
    def handle_ensure_user_dir_request(self, requester_ip, user_root, session_user=None):
        if not self.state_mgr.get_file_info(user_root):
            self.state_mgr.file_map[user_root] = {'type': 'dir', 'mtime': time.time(), 'replicas': self.cluster_comm.cluster_ips}
        return {'status': 'ok'}
    
    # --- FUNCIONES AUXILIARES ---

    def calculate_replica_nodes(self, filename: str, size: int = 0) -> List[str]:
        all_ips = sorted(self.cluster_comm.cluster_ips)
        if not all_ips: return []
        if len(all_ips) <= 3: return all_ips
        
        # Hash consistente simple
        file_hash = hashlib.md5(filename.encode()).hexdigest()
        start_idx = int(file_hash, 16) % len(all_ips)
        
        replicas = []
        for i in range(3):
            replicas.append(all_ips[(start_idx + i) % len(all_ips)])
            
        return list(set(replicas))

    def acquire_file_lock(self, path: str, holder_ip: str, lock_type: str = 'write') -> Tuple[bool, str]:
        success = self.lock_mgr.acquire_lock(path, holder_ip, lock_type)
        if success:
            return True, f"{path}_{holder_ip}_{lock_type}_{time.time()}"
        else:
            current = self.lock_mgr.locks.get(path, {})
            return False, f"Locked by {current.get('holder', 'unknown')}"
    
    def release_file_lock(self, path: str, holder_ip: str):
        return self.lock_mgr.release_lock(path, holder_ip)
    
    def _send_rename_order_to_nodes(self, old_path: str, new_path: str, 
                                nodes: List[str], operation_id: str, 
                                item_type: str = 'file', files_in_dir: List = None):
        """Envía orden de renombrar a los nodos réplica."""
        message = {
            'type': 'FS_ORDER',
            'command': 'RENAME',
            'old_path': old_path,
            'new_path': new_path,
            'operation_id': operation_id,
            'item_type': item_type,
            'is_directory': item_type == 'dir',
            'timestamp': time.time(),
            'must_execute_on_all': item_type == 'dir'  # Flag para directorios
        }
        
        # Si es un directorio, incluir información de archivos dentro
        if item_type == 'dir' and files_in_dir:
            # Solo incluir metadatos básicos para no hacer el mensaje muy grande
            message['files_in_dir'] = [
                {
                    'old_path': path,
                    'new_path': new_path.rstrip('/') + '/' + os.path.relpath(path, old_path),
                    'type': info.get('type', 'file')
                }
                for path, info in files_in_dir[:10]  # Limitar a 10 para no sobrecargar
            ]
            message['total_files_in_dir'] = len(files_in_dir)
        
        local_ip = self.cluster_comm.local_ip
        
        print(f"[LEADER] Enviando orden RENAME a {len(nodes)} nodos: {old_path} -> {new_path}")
        
        for node_ip in nodes:
            if node_ip == local_ip:
                # Ejecutar localmente en un hilo
                from ftp.sidecar import handle_fs_order
                threading.Thread(
                    target=handle_fs_order,
                    args=(message,),
                    daemon=True
                ).start()
                print(f"[LEADER] Orden de renombrar enviada localmente: {old_path} -> {new_path}")
            else:
                # Enviar a nodo remoto
                threading.Thread(
                    target=self.cluster_comm.send_message,
                    args=(node_ip, message, False),
                    daemon=True
                ).start()
                print(f"[LEADER] Orden de renombrar enviada a {node_ip}: {old_path} -> {new_path}")

    def _send_operation_to_nodes(self, command, path, nodes, op_id, wait=True):
        """
        Envía operaciones a los nodos.
        Args:
            wait (bool): Si es True, el líder espera a que los nodos confirmen (o fallen) 
                         antes de retornar. Esto evita que el cliente mande RMD antes 
                         de que DELE termine.
        """
        msg = {'type': 'FS_ORDER', 'command': command, 'path': path, 'operation_id': op_id}
        local = self.cluster_comm.local_ip
        
        active_threads = []

        for ip in nodes:
            if ip == local:
                from ftp.sidecar import handle_fs_order
                t = threading.Thread(target=handle_fs_order, args=(msg,), daemon=True)
                t.start()
                active_threads.append(t)
            else:
                # Usamos expect_response=wait para que el envío sea bloqueante si es necesario
                t = threading.Thread(
                    target=self.cluster_comm.send_message, 
                    args=(ip, msg, wait), 
                    daemon=True
                )
                t.start()
                active_threads.append(t)

        # Si wait es True, esperamos a que todos los hilos terminen su trabajo
        if wait:
            for t in active_threads:
                t.join(timeout=5.0) # Timeout de seguridad para no bloquear eternamente

    def _send_replicate_order(self, target_ip: str, file_path: str, source_ip: str, size: int, file_hash: str, operation_id: str):
        """Envía orden de replicación a un nodo (puede ser local o remoto)."""
        message = {
            'type': 'FS_ORDER',
            'command': 'REPLICATE_FILE',
            'path': file_path,
            'source': source_ip,
            'size': size,
            'hash': file_hash,
            'operation_id': operation_id,
            'timestamp': time.time(),
            'is_repair': True
        }
        
        local_ip = self.cluster_comm.local_ip

        if target_ip == local_ip:
            print(f"[LEADER] Auto-replicación: Ordenando descarga local de {file_path} desde {source_ip}")
            # Importación local para evitar ciclos
            from ftp.sidecar import handle_fs_order
            threading.Thread(
                target=handle_fs_order,
                args=(message,),
                daemon=True
            ).start()
        else:
            print(f"[LEADER] Replicación remota: Ordenando a {target_ip} descargar {file_path} desde {source_ip}")
            threading.Thread(
                target=self.cluster_comm.send_message,
                args=(target_ip, message, False),
                daemon=True
            ).start()

    def _check_locks_recursive(self, path: str) -> Tuple[bool, List[str]]:
            """
            Verifica si hay locks activos en un path o sus descendientes.
            
            Returns:
                (has_locks, locked_paths): 
                    - has_locks: True si hay algún lock activo
                    - locked_paths: Lista de rutas bloqueadas
            """
            locked_paths = []
            
            # Verificar el path mismo
            with self.lock_mgr.lock:
                if path in self.lock_mgr.locks:
                    locked_paths.append(path)
            
            # Para directorios, verificar todos los archivos dentro
            with self.state_mgr.lock:
                dir_prefix = path.rstrip('/') + '/'
                
                for file_path in self.state_mgr.file_map.keys():
                    # Si es un archivo dentro del directorio
                    if file_path.startswith(dir_prefix) or file_path == path:
                        with self.lock_mgr.lock:
                            if file_path in self.lock_mgr.locks:
                                lock_info = self.lock_mgr.locks[file_path]
                                locked_paths.append(f"{file_path} (locked by {lock_info['holder']})")
            
            return len(locked_paths) > 0, locked_paths

    # --- LLAMADAS EXTERNAS ---

    def handle_node_join(self, new_ip: str):
        """Cuando se une un nodo, verificamos todo el sistema."""
        print(f"[LEADER] Nodo unido {new_ip}. Ejecutando chequeo de replicación...")
        self.check_replication_status()

    def handle_node_failure(self, failed_ip: str):
        """Cuando un nodo falla, limpiamos y reparamos."""
        print(f"[LEADER] Nodo fallido {failed_ip}. Limpiando mapa y reparando...")
        
        # Liberar todos los locks que tenía este nodo
        with self.lock_mgr.lock:
            locks_to_release = []
            for path, lock_info in self.lock_mgr.locks.items():
                if lock_info.get('holder') == failed_ip:
                    locks_to_release.append(path)
            
            for path in locks_to_release:
                print(f"[LEADER] Liberando lock de nodo fallido {failed_ip} en {path}")
                del self.lock_mgr.locks[path]

        with self.state_mgr.lock:
            for path, info in self.state_mgr.file_map.items():
                if 'replicas' in info and failed_ip in info['replicas']:
                    info['replicas'].remove(failed_ip)
        
        self.check_replication_status()

    def check_replication_status(self):
        """
        Verifica la salud de todos los archivos. 
        Si faltan réplicas, las crea. Si sobran (por nodos muertos), limpia el mapa.
        """
        # Obtenemos nodos disponibles (excluyendo 'localhost' si se colara)
        available_nodes = [ip for ip in self.cluster_comm.cluster_ips if ip != '127.0.0.1']
        
        # Si hay menos de 2 nodos, es difícil replicar, pero si soy el único, debo tenerlo todo.
        if not available_nodes: 
            return

        print(f"[LEADER-CHECK] Verificando salud con nodos disponibles: {available_nodes}")

        files_to_repair = []

        with self.state_mgr.lock:
            for path, info in self.state_mgr.file_map.items():
                if info.get('type') == 'file':
                    current_replicas = info.get('replicas', [])
                    
                    # 1. Limpiar réplicas fantasma (nodos que ya no están en el cluster)
                    valid_replicas = [ip for ip in current_replicas if ip in available_nodes]
                    
                    # Si hubo cambios, actualizamos el mapa inmediatamente
                    if len(valid_replicas) != len(current_replicas):
                        print(f"[LEADER-CHECK] Limpiando réplicas muertas para {path}. Eran: {current_replicas} -> Son: {valid_replicas}")
                        info['replicas'] = valid_replicas

                    # 2. Verificar si necesitamos más réplicas
                    # Objetivo: 3 réplicas, o el total de nodos si hay menos de 3
                    target_count = min(3, len(available_nodes))
                    
                    if len(valid_replicas) < target_count:
                        # Añadimos a la lista para reparar fuera del lock principal si es posible,
                        # o procesamos aquí. Para simplificar, procesamos aquí pero enviamos hilos.
                        files_to_repair.append((path, valid_replicas.copy()))

        # Procesar reparaciones
        for path, valid_replicas in files_to_repair:
            self._repair_file(path, valid_replicas, available_nodes)

    def _repair_file(self, path: str, current_replicas: List[str], available_nodes: List[str]):
        """
        Coordina la replicación de un archivo hacia nuevos nodos.
        """
        if not current_replicas:
            print(f"[LEADER-REPAIR] CRÍTICO: Archivo {path} perdido completamente (0 réplicas vivas).")
            return

        # El nodo fuente será el primero de los que tienen el archivo
        source_node = current_replicas[0]
        
        # Candidatos: Nodos disponibles que NO tienen el archivo
        candidates = [ip for ip in available_nodes if ip not in current_replicas]
        
        if not candidates:
            print(f"[LEADER-REPAIR] No hay candidatos disponibles para replicar {path}")
            return

        # Cuántos faltan
        target_count = min(3, len(available_nodes))
        needed = target_count - len(current_replicas)
        
        # Seleccionar objetivos
        targets = candidates[:needed]
        
        print(f"[LEADER-REPAIR] Reparando {path}. Fuente: {source_node}. Destinos: {targets}")
        
        file_info = self.state_mgr.get_file_info(path)
        
        for target_ip in targets:
            # 1. Actualización OPTIMISTA del mapa global
            # Marcamos que el nodo ya tiene la réplica para evitar que el próximo check
            # lance otra orden antes de que esta termine.
            with self.state_mgr.lock:
                if path in self.state_mgr.file_map:
                    self.state_mgr.file_map[path]['replicas'].append(target_ip)

            # 2. Enviar orden
            self._send_replicate_order(
                target_ip, 
                path, 
                source_node, 
                file_info.get('size', 0), 
                file_info.get('hash', ''), 
                f"repair_{int(time.time())}"
            )

    def run_garbage_collection(self):
        """
        Ejecuta garbage collection en TODOS los nodos del cluster.
        Escanea discos y elimina archivos/directorios obsoletos.
        Maneja el caso local (Líder) y remoto por separado.
        """
        print("[LEADER-GC] Iniciando garbage collection global del cluster...")
        
        all_nodes = self.cluster_comm.cluster_ips.copy()
        root_path = os.environ.get('SERVER_ROOT', '/app/server/data')
        
        for node_ip in all_nodes:
            try:
                print(f"[LEADER-GC] Analizando nodo {node_ip}...")
                
                node_logs = []
                disk_scan = None
                
                # --- CASO 1: NODO LOCAL (LÍDER) ---
                if node_ip == self.cluster_comm.local_ip:
                    print(f"[LEADER-GC] (Local) Obteniendo logs y escaneo de disco internamente...")
                    # 1. Obtener logs directamente de memoria
                    node_logs = self.state_mgr.get_full_log_as_dict()
                    
                    # 2. Escanear disco directamente
                    disk_scan = self.state_mgr.scan_local_filesystem(root_path)
                    print(f"[LEADER-GC] (Local) Escaneados {len(disk_scan)} elementos en disco")

                # --- CASO 2: NODO REMOTO ---
                else:
                    # 1. Solicitar logs via red
                    resp_logs = self.cluster_comm.send_message(
                        node_ip, 
                        {'type': 'REQUEST_LOGS'}, 
                        expect_response=True
                    )
                    
                    if not resp_logs or resp_logs.get('status') != 'ok':
                        print(f"[LEADER-GC] No se pudieron obtener logs de {node_ip}")
                        continue
                    
                    node_logs = resp_logs.get('log', [])
                    
                    # 2. Solicitar escaneo de disco via red
                    resp_scan = self.cluster_comm.send_message(
                        node_ip,
                        {
                            'type': 'REQUEST_DISK_SCAN',
                            'root_path': root_path
                        },
                        expect_response=True
                    )
                    
                    if resp_scan and resp_scan.get('status') == 'ok':
                        disk_scan = resp_scan.get('disk_scan', {})
                        print(f"[LEADER-GC] Escaneados {len(disk_scan)} elementos en {node_ip}")

                # --- 3. ANÁLISIS COMÚN ---
                # Usamos la misma lógica de análisis para ambos casos
                analysis = self.state_mgr.analyze_node_state(node_logs, node_ip, disk_scan, safe_mode=False)
                
                if not analysis['has_inconsistencies']:
                    print(f"[LEADER-GC] Nodo {node_ip} está limpio")
                    continue
                
                # 4. Filtrar solo comandos de limpieza (prioridad 1) - Zombies
                zombies = [cmd for cmd in analysis['inconsistencies'] 
                           if cmd.get('priority', 99) == 1]
                
                if not zombies:
                    print(f"[LEADER-GC] Nodo {node_ip} no tiene zombies")
                    continue
                
                print(f"[LEADER-GC] Detectados {len(zombies)} zombies en {node_ip}")
                
                # --- EJECUCIÓN DE LIMPIEZA ---
                
                if node_ip == self.cluster_comm.local_ip:
                    # EJECUCIÓN LOCAL
                    print(f"[LEADER-GC] (Local) Ejecutando limpieza interna...")
                    # Importación local para evitar ciclos
                    from ftp.sidecar import handle_sync_commands
                    
                    # Simulamos el mensaje para reutilizar la lógica de sidecar
                    local_msg = {
                        'commands': zombies,
                        'phase': 'garbage_collection'
                    }
                    result = handle_sync_commands(local_msg)
                    stats = result.get('stats', {})
                    print(f"[LEADER-GC] (Local): {stats.get('zombies_deleted', 0)} zombies eliminados")
                    
                else:
                    # EJECUCIÓN REMOTA
                    cleanup_response = self.cluster_comm.send_message(
                        node_ip,
                        {
                            'type': 'SYNC_COMMANDS',
                            'commands': zombies,
                            'phase': 'garbage_collection',
                            'block_until_complete': True
                        },
                        expect_response=True
                    )
                    
                    if cleanup_response and cleanup_response.get('status') == 'ok':
                        stats = cleanup_response.get('stats', {})
                        print(f"[LEADER-GC] {node_ip}: {stats.get('zombies_deleted', 0)} zombies eliminados")
                    else:
                        print(f"[LEADER-GC] Error en limpieza remota de {node_ip}")
                    
            except Exception as e:
                print(f"[LEADER-GC] Error procesando {node_ip}: {e}")
                import traceback
                traceback.print_exc()
        
        print("[LEADER-GC] Garbage collection global completado")

# Singleton para LeaderOperations
_leader_ops_global = None

def get_leader_operations(cluster_comm=None, bully=None):
    global _leader_ops_global
    if _leader_ops_global is None and cluster_comm and bully:
        _leader_ops_global = LeaderOperations(cluster_comm, bully)
    return _leader_ops_global

def process_local_leader_request(message_type: str, data: Dict, cluster_comm=None, bully=None) -> Dict:
    if cluster_comm is None or bully is None:
        from ftp.sidecar import get_global_cluster_comm, get_global_bully
        cluster_comm = get_global_cluster_comm()
        bully = get_global_bully()
        
    ops = get_leader_operations(cluster_comm, bully)
    
    # Mapeo simple de strings a métodos
    method_name = f"handle_{message_type.lower()}_request"
    if hasattr(ops, method_name):
        method = getattr(ops, method_name)
        try:
            if message_type in ['MKD', 'RMD', 'DELE', 'RETR', 'RNFR', 'RNTO', 'ENSURE_USER_DIR']:
                if message_type == 'RNFR':
                    return ops.handle_rnfr_request(data.get('requester'), data.get('path'), data.get('session_user'))
                elif message_type == 'RNTO':
                    return ops.handle_rnto_request(
                        data.get('requester'), 
                        data.get('old_path'), 
                        data.get('new_path'), 
                        data.get('lock_id'), 
                        data.get('session_user')
                    )
                else:
                    return method(data.get('requester'), data.get('path') or data.get('user_root'), data.get('session_user'))
            elif message_type == 'APPE':
                return ops.handle_appe_request(data.get('requester'), data.get('path'), data.get('size'), data.get('session_user'))
            elif message_type == 'RELEASE_LOCK':
                return ops.handle_release_lock_request(data.get('requester'), data.get('path'), data.get('session_user'))
            elif message_type == 'STOR':
                return method(data.get('requester'), data.get('path'), data.get('size', 0), data.get('hash'), data.get('session_user'), data.get('append', False))
            elif message_type in ['LIST', 'NLST', 'CWD', 'PWD', 'CDUP']:
                 return method(data.get('requester'), data.get('abs_path'), data.get('user_root'), data.get('session_user'))
            elif message_type == 'RNTO':
                return method(data.get('requester'), data.get('old_path'), data.get('new_path'), data.get('lock_id'), data.get('session_user'))
            elif message_type == 'COMPLETION':
                return ops.handle_completion(data.get('operation_id'), data.get('success'), data.get('message'))
        except Exception as e:
            print(f"Error invoking {method_name}: {e}")
            return {'status': 'error', 'message': str(e)}

    return {'status': 'error', 'message': f'Unknown type {message_type}'}
