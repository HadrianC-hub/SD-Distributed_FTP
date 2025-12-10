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

