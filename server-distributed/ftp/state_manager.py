import os
import json
import random
import time
import threading
from typing import List, Dict, Set
from collections import defaultdict
from ftp.paths import calculate_file_hash

# Definición de tipos de operaciones
OP_MKD = 'MKD'
OP_RMD = 'RMD'
OP_STOR = 'STOR'
OP_DELE = 'DELE'
OP_RN = 'RN'
OP_REPAIR = 'REPAIR'
OP_APPE = 'APPE'


class LogEntry:
    def __init__(self, index: int, op_type: str, path: str, timestamp: float, metadata: Dict = None):
        self.index = index
        self.op_type = op_type
        self.path = path
        self.timestamp = timestamp
        self.metadata = metadata or {}
        self.origin_node_ip = None
        self.partition_epoch = metadata.get('partition_epoch', 0) if metadata else 0

    def to_dict(self):
        return {
            'index': self.index,
            'op_type': self.op_type,
            'path': self.path,
            'timestamp': self.timestamp,
            'metadata': self.metadata,
            'partition_epoch': self.partition_epoch
        }

    @staticmethod
    def from_dict(data):
        entry = LogEntry(
            data.get('index', 0), data['op_type'], 
            data['path'], data['timestamp'], data.get('metadata', {})
        )
        entry.partition_epoch = data.get('partition_epoch', 0)
        return entry


class LockManager:
    def __init__(self):
        self.locks = {}
        self.lock = threading.Lock()
    
    def acquire_lock(self, path, holder, mode='write'):
        with self.lock:
            if path in self.locks and self.locks[path]['holder'] != holder: 
                return False
            self.locks[path] = {'holder': holder, 'ts': time.time()}
            return True
    
    def release_lock(self, path, holder):
        with self.lock:
            if path in self.locks and self.locks[path]['holder'] == holder:
                del self.locks[path]
                return True
            return False


class ConflictDetector:
    """Detecta y resuelve conflictos entre operaciones de diferentes particiones"""
    
    @staticmethod
    def detect_conflicts(operations_by_path: Dict[str, List[LogEntry]], 
                        node_final_states: Dict[str, Dict]) -> List[Dict]:
        """
        Detecta conflictos verificando si múltiples nodos tienen el mismo path
        pero con contenido diferente (hash diferente)
        """
        conflicts = []
        
        for path, ops in operations_by_path.items():
            conflict = ConflictDetector._analyze_file_conflict(
                path, ops, node_final_states
            )
            if conflict:
                conflicts.append(conflict)
        
        return conflicts
    
    @staticmethod
    def _analyze_file_conflict(path: str, ops: List[LogEntry], 
                               node_final_states: Dict[str, Dict]) -> Dict:
        """
        Analiza conflicto de archivos usando HASH del contenido final en cada nodo.
        """
        # Obtener nodos que tienen este archivo Y su hash
        file_hashes_by_node = {}
        file_sizes_by_node = {}
        nodes_with_file = set()
        
        for node_ip, node_state in node_final_states.items():
            if path in node_state:
                file_info = node_state[path]
                if file_info.get('exists', False):
                    nodes_with_file.add(node_ip)
                    file_hash = file_info.get('hash', '')
                    file_size = file_info.get('size', 0)
                    
                    # Solo contar si tenemos hash válido
                    if file_hash and len(file_hash) > 0:
                        file_hashes_by_node[node_ip] = file_hash
                        file_sizes_by_node[node_ip] = file_size
        
        # Si no hay al menos un nodo con hash, no podemos detectar conflicto
        if len(file_hashes_by_node) == 0:
            return None
        
        # Si solo un nodo tiene el archivo con hash, verificar contra otros nodos
        if len(file_hashes_by_node) == 1 and len(nodes_with_file) > 1:
            return None
        
        # Agrupar nodos por hash (nodos con mismo hash tienen misma versión)
        nodes_by_hash = defaultdict(list)
        for node_ip, file_hash in file_hashes_by_node.items():
            nodes_by_hash[file_hash].append(node_ip)
        
        # Si todos tienen el mismo hash, no hay conflicto
        if len(nodes_by_hash) == 1:
            return None
        
        # HAY CONFLICTO: múltiples versiones del archivo
        print(f"[CONFLICT] CONFLICTO DETECTADO: {len(nodes_by_hash)} versiones diferentes")
        
        # Obtener información de operaciones
        stor_ops = [op for op in ops if op.op_type == OP_STOR]
        appe_ops = [op for op in ops if op.op_type == OP_APPE]
        base_size = stor_ops[0].metadata.get('size', 0) if stor_ops else 0
        
        # Crear información de versiones
        versions = {}
        for version_num, (file_hash, nodes) in enumerate(sorted(nodes_by_hash.items()), 1):
            version_size = file_sizes_by_node.get(nodes[0], 0)
            total_appended = version_size - base_size
            
            # Obtener operaciones de estos nodos
            version_ops = [op for op in ops if op.origin_node_ip in nodes]
            last_ts = max(op.timestamp for op in version_ops) if version_ops else time.time()
            
            versions[version_num] = {
                'hash': file_hash,
                'nodes': nodes,
                'final_size': version_size,
                'base_size': base_size,
                'total_appended': total_appended,
                'last_timestamp': last_ts,
                'operations': version_ops
            }
            
            print(f"[CONFLICT]     Versión {version_num}: hash={file_hash[:16]}..., "
                  f"tamaño={version_size}, nodos={nodes}")
        
        # Determinar tipo de conflicto
        if len(appe_ops) > 0:
            conflict_type = 'concurrent_appends_by_hash'
        else:
            conflict_type = 'file_content_mismatch'
        
        return {
            'type': conflict_type,
            'path': path,
            'resolution': 'create_versions_by_hash',
            'reason': f'Múltiples nodos tienen versiones diferentes del mismo archivo',
            'versions': versions,
            'base_size': base_size,
            'operations': ops,
            'nodes_with_file': list(nodes_with_file)
        }
    
    @staticmethod
    def resolve_conflicts(conflicts: List[Dict], file_map: Dict) -> List[Dict]:
        """
        Resuelve conflictos creando versiones basadas en hash
        """
        actions = []
        
        for conflict in conflicts:
            if conflict['resolution'] == 'create_versions_by_hash':
                path = conflict['path']
                versions = conflict['versions']
                
                print(f"[RESOLVE] Resolviendo conflicto en {path} con {len(versions)} versiones")
                
                # Todos los nodos involucrados
                all_nodes = set(conflict.get('nodes_with_file', []))
                for version_info in versions.values():
                    all_nodes.update(version_info['nodes'])
                
                print(f"[RESOLVE] Nodos involucrados: {all_nodes}")
                
                # Crear entrada en file_map para cada versión
                for version_num, version_info in versions.items():
                    base, ext = os.path.splitext(path)
                    version_hash_short = version_info['hash'][:8]
                    versioned_path = f"{base}_v{version_num}_{version_hash_short}{ext}"
                    
                    nodes_with_this_version = version_info['nodes']
                    final_size = version_info['final_size']
                    
                    print(f"[RESOLVE] Versión {version_num}: {versioned_path}")
                    print(f"[RESOLVE]   - Tamaño: {final_size}, Nodos: {nodes_with_this_version}")
                    
                    # Actualizar file_map - mantener SOLO los nodos que tienen esta versión
                    file_map[versioned_path] = {
                        'type': 'file',
                        'mtime': version_info['last_timestamp'],
                        'size': final_size,
                        'hash': version_info['hash'],
                        'replicas': nodes_with_this_version,
                        'conflict_resolved': True,
                        'resolution_type': conflict['type'],
                        'version': version_num,
                        'original_path': path
                    }
                    
                    # FASE 1: Renombrar en nodos que tienen esta versión
                    for node_ip in nodes_with_this_version:
                        if node_ip in all_nodes:
                            actions.append({
                                'action': 'rename_local_to_version',
                                'node': node_ip,
                                'old_path': path,
                                'new_path': versioned_path,
                                'version': version_num,
                                'expected_hash': version_info['hash'],
                                'expected_size': final_size,
                                'priority': 1
                            })
                
                # Eliminar entrada original
                if path in file_map:
                    del file_map[path]
                    print(f"[RESOLVE] Entrada original eliminada del file_map: {path}")
        
        return actions


class StateManager:
    def __init__(self, node_id: str, persistence_file='node_state.json'):
        self.node_id = node_id
        self.persistence_file = persistence_file
        self.lock = threading.Lock()
        self.op_log: List[LogEntry] = []
        self.file_map: Dict[str, Dict] = {}
        self.partition_epoch = 0
        self.conflict_detector = ConflictDetector()
        self.load_state()

    # --- MANEJO DE ESTADO ---

    def load_state(self):
        if os.path.exists(self.persistence_file):
            try:
                with open(self.persistence_file, 'r') as f:
                    data = json.load(f)
                    self.op_log = [LogEntry.from_dict(e) for e in data.get('log', [])]
                    self.partition_epoch = data.get('partition_epoch', 0)
                    for e in self.op_log: 
                        self._apply_entry_locally(e)
            except: 
                pass

    def save_state(self):
        try:
            data = {
                'node_id': self.node_id, 
                'log': [e.to_dict() for e in self.op_log],
                'partition_epoch': self.partition_epoch
            }
            with open(self.persistence_file, 'w') as f:
                json.dump(data, f)
        except: 
            pass

    def increment_partition_epoch(self):
        with self.lock:
            self.partition_epoch += 1
            print(f"[STATE] Partition epoch incrementado a {self.partition_epoch}")
            self.save_state()

    # --- CARGA DE OPERACIONES ---

    def append_operation(self, op_type: str, path: str, metadata: Dict = None) -> LogEntry:
        with self.lock:
            if metadata is None:
                metadata = {}
            metadata['partition_epoch'] = self.partition_epoch
            
            entry = LogEntry(len(self.op_log), op_type, path, time.time(), metadata)
            self.op_log.append(entry)
            self.save_state()
            self._apply_entry_locally(entry)
            return entry

    # --- MANEJO DE ENTRADAS DEL LOG ---

    def _apply_entry_locally(self, entry: LogEntry):
        path = entry.path
        if entry.op_type == OP_MKD:
            if path not in self.file_map:
                self.file_map[path] = {'type': 'dir', 'replicas': []}
        elif entry.op_type == OP_RMD:
            if path in self.file_map:
                del self.file_map[path]
            # Eliminar también contenido del directorio
            dir_prefix = path.rstrip('/') + '/'
            keys_to_delete = [k for k in self.file_map.keys() if k.startswith(dir_prefix)]
            for k in keys_to_delete:
                del self.file_map[k]
        elif entry.op_type == OP_DELE:
            if path in self.file_map:
                del self.file_map[path]
        elif entry.op_type == OP_REPAIR:
            metadata = entry.metadata
            if path in self.file_map and 'new_replicas' in metadata:
                self.file_map[path]['replicas'] = metadata['new_replicas']
        elif entry.op_type == 'RN':
            metadata = entry.metadata
            old_path = metadata.get('old_path')
            new_path = entry.path
            
            if old_path in self.file_map:
                self.file_map[new_path] = self.file_map[old_path]
                del self.file_map[old_path]
                
                if self.file_map[new_path].get('type') == 'dir':
                    old_prefix = old_path.rstrip('/') + '/'
                    new_prefix = new_path.rstrip('/') + '/'
                    
                    keys_to_update = [k for k in self.file_map.keys() 
                                    if k.startswith(old_prefix)]
                    
                    for old_key in keys_to_update:
                        relative_part = old_key[len(old_prefix):]
                        new_key = new_prefix + relative_part
                        
                        if old_key in self.file_map:
                            self.file_map[new_key] = self.file_map[old_key]
                            del self.file_map[old_key]
                            
    # --- RECONSTRUCCION DE ESQUEMA GLOBAL CON DETECCIÓN DE CONFLICTOS ---

    def merge_external_logs(self, all_logs_with_ips: List[Dict]):
        """
        Fusión PRIORIDAD A DISCO: Lo que existe en disco tiene prioridad absoluta
        """

        with self.lock:
            print("[STATE] === INICIO DE FUSIÓN (Prioridad a Disco) ===")
            
            nodes_by_ip = {}
            
            # 1. Recolectar TODOS los archivos que existen en disco en TODOS los nodos
            all_files_on_disk = {}  # path -> {node_ip: {hash, size, ...}}
            
            for node_data in all_logs_with_ips:
                node_ip = node_data['node_ip']
                raw_log = node_data['log']
                raw_scan = node_data.get('scan', {})
                
                # Asegurar que disk_scan sea un diccionario
                if isinstance(raw_scan, list):
                    disk_scan_dict = {item['path']: item for item in raw_scan}
                else:
                    disk_scan_dict = raw_scan if raw_scan else {}
                
                print(f"[STATE] Procesando nodo {node_ip}: {len(raw_log)} ops, {len(disk_scan_dict)} archivos en disco")
                
                nodes_by_ip[node_ip] = {
                    'operations': [LogEntry.from_dict(e) for e in sorted(raw_log, key=lambda x: x.get('timestamp', 0))],
                    'disk_scan': disk_scan_dict
                }
                
                # Registrar archivos en disco
                for path, file_info in disk_scan_dict.items():
                    if file_info.get('type') == 'file':
                        if path not in all_files_on_disk:
                            all_files_on_disk[path] = {}
                        all_files_on_disk[path][node_ip] = {
                            'hash': file_info.get('hash', ''),
                            'size': file_info.get('size', 0),
                            'mtime': file_info.get('mtime', time.time())
                        }
            
            print(f"[STATE] Archivos únicos en disco: {len(all_files_on_disk)}")
            
            # 2. Detectar conflictos basándose en archivos en disco
            conflicts = []
            for path, nodes_data in all_files_on_disk.items():
                # Agrupar nodos por hash
                nodes_by_hash = defaultdict(list)
                for node_ip, file_data in nodes_data.items():
                    file_hash = file_data.get('hash', '')
                    if file_hash:
                        nodes_by_hash[file_hash].append(node_ip)
                
                # Si hay múltiples versiones del mismo archivo
                if len(nodes_by_hash) > 1:
                    print(f"[STATE] Conflicto detectado en {path}: {len(nodes_by_hash)} versiones")
                    
                    versions = {}
                    for version_num, (file_hash, node_list) in enumerate(sorted(nodes_by_hash.items()), 1):
                        # Obtener tamaño del primer nodo con este hash
                        first_node = node_list[0]
                        file_data = nodes_data[first_node]
                        
                        versions[version_num] = {
                            'hash': file_hash,
                            'nodes': node_list,
                            'final_size': file_data['size'],
                            'last_timestamp': file_data['mtime']
                        }
                        print(f"[STATE]   Versión {version_num}: hash={file_hash[:16]}..., nodos={node_list}")
                    
                    conflicts.append({
                        'type': 'file_content_mismatch',
                        'path': path,
                        'resolution': 'create_versions_by_hash',
                        'versions': versions,
                        'nodes_with_file': list(nodes_data.keys())
                    })
            
            print(f"[STATE] Conflictos detectados: {len(conflicts)}")
            
            # 3. Construir file_map DESDE DISCO
            temp_file_map = {}
            paths_in_conflict = {c['path'] for c in conflicts}
            
            # A. Agregar archivos SIN conflicto
            for path, nodes_data in all_files_on_disk.items():
                if path in paths_in_conflict:
                    continue  # Los manejaremos con las versiones
                
                # Todos los nodos tienen el mismo archivo (sin conflicto)
                replica_nodes = list(nodes_data.keys())
                first_node = replica_nodes[0]
                file_info = nodes_data[first_node]
                
                temp_file_map[path] = {
                    'type': 'file',
                    'mtime': file_info['mtime'],
                    'size': file_info['size'],
                    'hash': file_info['hash'],
                    'replicas': replica_nodes
                }
                print(f"[STATE] Archivo sin conflicto: {path}, réplicas: {replica_nodes}")
            
            # B. Agregar directorios del disco
            for node_ip, node_data in nodes_by_ip.items():
                for path, item_info in node_data['disk_scan'].items():
                    if item_info.get('type') == 'dir' and path not in temp_file_map:
                        temp_file_map[path] = {
                            'type': 'dir',
                            'mtime': item_info.get('mtime', time.time()),
                            'replicas': set()
                        }
                    
                    if item_info.get('type') == 'dir' and path in temp_file_map:
                        temp_file_map[path]['replicas'].add(node_ip)
            
            # C. Resolver conflictos y crear versiones
            conflict_actions = []
            if conflicts:
                for conflict in conflicts:
                    path = conflict['path']
                    versions = conflict['versions']
                    
                    print(f"[STATE] Resolviendo conflicto en {path} con {len(versions)} versiones")
                    
                    for version_num, version_info in versions.items():
                        base, ext = os.path.splitext(path)
                        version_hash_short = version_info['hash'][:8]
                        versioned_path = f"{base}_v{version_num}_{version_hash_short}{ext}"
                        
                        nodes_with_this_version = version_info['nodes']
                        
                        # Crear entrada para la versión
                        temp_file_map[versioned_path] = {
                            'type': 'file',
                            'mtime': version_info['last_timestamp'],
                            'size': version_info['final_size'],
                            'hash': version_info['hash'],
                            'replicas': nodes_with_this_version,
                            'conflict_resolved': True,
                            'original_path': path
                        }
                        
                        print(f"[STATE] Versión creada: {versioned_path}, réplicas: {nodes_with_this_version}")
                        
                        # Crear acciones de renombre para nodos que tienen el original
                        for node_ip in nodes_with_this_version:
                            conflict_actions.append({
                                'action': 'rename_local_to_version',
                                'node': node_ip,
                                'old_path': path,
                                'new_path': versioned_path,
                                'version': version_num,
                                'expected_hash': version_info['hash'],
                                'expected_size': version_info['final_size'],
                                'priority': 1
                            })
            
            # D. Convertir sets a listas
            for path, info in temp_file_map.items():
                if isinstance(info.get('replicas'), set):
                    info['replicas'] = list(info['replicas'])
            
            # 4. Actualizar estado
            self.file_map = temp_file_map
            self.save_state()
            
            print(f"[STATE] Fusión completada: {len(temp_file_map)} archivos/dirs, {len(conflict_actions)} acciones")
            return {}, conflict_actions

    # --- TRACKING DE RÉPLICAS ---

    def get_all_replicas_for_path(self, path: str) -> Set[str]:
        with self.lock:
            replicas = set()
            if path in self.file_map:
                replicas.update(self.file_map[path].get('replicas', []))
            for entry in self.op_log:
                if entry.path == path and entry.origin_node_ip:
                    replicas.add(entry.origin_node_ip)
            return replicas

    # --- LLAMADAS EXTERNAS Y UTILIDADES ---

    def scan_local_filesystem(self, root_dir: str) -> Dict[str, Dict]:
        """Escanea el sistema de archivos local incluyendo HASH"""
        local_files = {}
        
        try:
            for dirpath, dirnames, filenames in os.walk(root_dir):
                if dirpath != root_dir:
                    local_files[dirpath] = {
                        'type': 'dir',
                        'mtime': os.path.getmtime(dirpath),
                        'exists_on_disk': True
                    }
                
                for filename in filenames:
                    if filename.endswith('.tmp') or '.delta.' in filename:
                        continue
                        
                    filepath = os.path.join(dirpath, filename)
                    try:
                        stat = os.stat(filepath)
                        file_hash = calculate_file_hash(filepath)
                        local_files[filepath] = {
                            'type': 'file',
                            'size': stat.st_size,
                            'mtime': stat.st_mtime,
                            'hash': file_hash,
                            'exists': True,
                            'exists_on_disk': True
                        }
                    except Exception as e:
                        print(f"[SCAN] Error al leer {filepath}: {e}")
                        
        except Exception as e:
            print(f"[SCAN] Error escaneando {root_dir}: {e}")
        
        return local_files

    def analyze_node_state(self, node_logs: List[Dict], node_ip: str, 
                          disk_scan: Dict[str, Dict] = None) -> Dict:
        """Analiza estado del nodo para sincronización"""
        with self.lock:
            node_state = disk_scan if disk_scan else {}
            inconsistencies = []
            
            # Detectar archivos faltantes
            for path, global_info in self.file_map.items():
                if global_info.get('type') == 'file':
                    replicas = global_info.get('replicas', [])
                    
                    if node_ip in replicas and path not in node_state:
                        inconsistencies.append({
                            'command': 'CREATE_FILE',
                            'path': path,
                            'replicas': replicas,
                            'size': global_info.get('size', 0),
                            'hash': global_info.get('hash', ''),
                            'reason': 'missing_replica',
                            'priority': 2
                        })
            
            # Detectar archivos zombie
            for path, node_info in node_state.items():
                if node_info.get('type') == 'file':
                    if path in self.file_map:
                        replicas = self.file_map[path].get('replicas', [])
                        if node_ip not in replicas:
                            inconsistencies.append({
                                'command': 'DELETE_ZOMBIE',
                                'path': path,
                                'reason': 'node_not_in_replica_list',
                                'safe_to_delete': False
                            })
                    else:
                        inconsistencies.append({
                            'command': 'DELETE_ZOMBIE',
                            'path': path,
                            'reason': 'file_not_in_global_state',
                            'safe_to_delete': False
                        })
            
            return {
                'has_inconsistencies': len(inconsistencies) > 0,
                'inconsistencies': inconsistencies
            }

    def get_full_log_as_dict(self):
        with self.lock:
            return [e.to_dict() for e in self.op_log]

    def get_file_info(self, path):
        with self.lock:
            return self.file_map.get(path)

    def ensure_user_directory(self, user_root: str):
        with self.lock:
            if user_root not in self.file_map:
                self.file_map[user_root] = {
                    'type': 'dir',
                    'mtime': time.time(),
                    'created_by': self.node_id,
                    'replicas': []
                }


# Singleton Global
_state_manager_instance = None
def get_state_manager(node_id=None):
    global _state_manager_instance
    if _state_manager_instance is None:
        _state_manager_instance = StateManager(node_id)
    return _state_manager_instance