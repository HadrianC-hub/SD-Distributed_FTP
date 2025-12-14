import os
import json
import time
import threading
from typing import List, Dict

# Definición de tipos de operaciones
OP_MKD = 'MKD'
OP_RMD = 'RMD'
OP_STOR = 'STOR'
OP_DELE = 'DELE'
OP_RN = 'RN'
OP_REPAIR = 'REPAIR'


class LogEntry:
    def __init__(self, index: int, op_type: str, path: str, timestamp: float, metadata: Dict = None):
        self.index = index
        self.op_type = op_type
        self.path = path
        self.timestamp = timestamp
        self.metadata = metadata or {}
        self.origin_node_ip = None

    def to_dict(self):
        return {
            'index': self.index,
            'op_type': self.op_type,
            'path': self.path,
            'timestamp': self.timestamp,
            'metadata': self.metadata
        }

    @staticmethod
    def from_dict(data):
        return LogEntry(
            data.get('index', 0), data['op_type'], 
            data['path'], data['timestamp'], data.get('metadata', {})
        )

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

class StateManager:
    def __init__(self, node_id: str, persistence_file='node_state.json'):
        self.node_id = node_id
        self.persistence_file = persistence_file
        self.lock = threading.Lock()
        self.op_log: List[LogEntry] = []
        self.file_map: Dict[str, Dict] = {}
        self.load_state()

    # --- MANEJO DE ESTADO ---

    def load_state(self):
        if os.path.exists(self.persistence_file):
            try:
                with open(self.persistence_file, 'r') as f:
                    data = json.load(f)
                    self.op_log = [LogEntry.from_dict(e) for e in data.get('log', [])]
                    for e in self.op_log: 
                        self._apply_entry_locally(e)
            except: 
                pass

    def save_state(self):
        try:
            data = {'node_id': self.node_id, 'log': [e.to_dict() for e in self.op_log]}
            with open(self.persistence_file, 'w') as f:
                json.dump(data, f)
        except: 
            pass

    # --- CARGA DE OPERACIONES ---

    def append_operation(self, op_type: str, path: str, metadata: Dict = None) -> LogEntry:
        with self.lock:
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

    # --- RECONSTRUCCION DE ESQUEMA GLOBAL ---

    def merge_external_logs(self, all_logs_with_ips: List[Dict]):
        with self.lock:
            print("[STATE] Reconstruyendo esquema global a partir de logs distribuidos...")
            
            temp_timeline = []
            
            for node_data in all_logs_with_ips:
                node_ip = node_data['node_ip']
                raw_log = node_data['log']
                for raw_entry in raw_log:
                    entry = LogEntry.from_dict(raw_entry)
                    entry.origin_node_ip = node_ip
                    temp_timeline.append(entry)
            
            temp_timeline.sort(key=lambda x: x.timestamp)
            
            self.file_map.clear()
            
            # Track de renombrados de directorios para actualizar rutas hijas
            dir_renames = []  # Lista de (old_path, new_path, timestamp)
            
            for entry in temp_timeline:
                path = entry.path
                origin = entry.origin_node_ip
                metadata = entry.metadata
                
                if entry.op_type == OP_MKD:
                    if path not in self.file_map:
                        self.file_map[path] = {
                            'type': 'dir',
                            'mtime': entry.timestamp,
                            'replicas': set()
                        }
                    self.file_map[path]['replicas'].add(origin)
                    
                elif entry.op_type == OP_RMD:
                    if path in self.file_map:
                        del self.file_map[path]
                    to_delete = [p for p in self.file_map.keys() if p.startswith(path + '/')]
                    for p in to_delete:
                        del self.file_map[p]
                        
                elif entry.op_type == OP_STOR:
                    # MEJORADO: Aplicar renombrados de directorios anteriores
                    actual_path = path
                    for old_dir, new_dir, rename_ts in dir_renames:
                        if entry.timestamp > rename_ts and path.startswith(old_dir.rstrip('/') + '/'):
                            # Este archivo está en un directorio que fue renombrado
                            rel_path = path[len(old_dir.rstrip('/') + '/'):]
                            actual_path = os.path.join(new_dir, rel_path).replace('\\', '/')
                            print(f"[MERGE] Aplicando renombrado de directorio: {path} -> {actual_path}")
                            break
                    
                    self.file_map[actual_path] = {
                        'type': 'file',
                        'mtime': entry.timestamp,
                        'size': metadata.get('size', 0),
                        'hash': metadata.get('hash', ''),
                        'replicas': set(metadata.get('replicas', [])),
                        'created_by': metadata.get('created_by', 'unknown')
                    }
                    self.file_map[actual_path]['replicas'].add(origin)
                    
                elif entry.op_type == OP_DELE:
                    if path in self.file_map:
                        del self.file_map[path]
                        
                elif entry.op_type == OP_RN:
                    old_path = metadata.get('old_path')
                    if old_path and old_path in self.file_map:
                        item_type = self.file_map[old_path].get('type', 'file')
                        
                        # Mover la entrada principal
                        self.file_map[path] = self.file_map[old_path]
                        self.file_map[path]['mtime'] = entry.timestamp
                        del self.file_map[old_path]
                        
                        # Si es un directorio, registrar el renombrado y actualizar todos los hijos
                        if item_type == 'dir':
                            dir_renames.append((old_path, path, entry.timestamp))
                            
                            old_prefix = old_path.rstrip('/') + '/'
                            new_prefix = path.rstrip('/') + '/'
                            
                            # Recolectar todas las rutas que necesitan actualizarse
                            keys_to_update = []
                            for key in list(self.file_map.keys()):
                                if key.startswith(old_prefix):
                                    keys_to_update.append(key)
                            
                            # Actualizar cada ruta
                            for old_key in keys_to_update:
                                relative_part = old_key[len(old_prefix):]
                                new_key = new_prefix + relative_part
                                
                                self.file_map[new_key] = self.file_map[old_key]
                                self.file_map[new_key]['mtime'] = entry.timestamp
                                del self.file_map[old_key]
                                
                                print(f"[MERGE] Renombrado de hijo: {old_key} -> {new_key}")
                    
                    # Asegurar que el origen se registre como réplica
                    if path in self.file_map:
                        self.file_map[path]['replicas'].add(origin)
            
            # Asegurar que el directorio root esté siempre en el esquema global
            root_path = '/app/server/data/root'
            if root_path not in self.file_map:
                self.file_map[root_path] = {
                    'type': 'dir',
                    'mtime': time.time(),
                    'replicas': set()
                }
                print(f"[STATE] Directorio root agregado al esquema global: {root_path}")
            
            # Convertir sets a listas
            for info in self.file_map.values():
                if 'replicas' in info and isinstance(info['replicas'], set):
                    info['replicas'] = list(info['replicas'])
            
            print(f"[STATE] Reconstrucción completada. {len(self.file_map)} objetos en el sistema.")
            print(f"[STATE] Se procesaron {len(dir_renames)} renombrados de directorios")

    # --- LLAMADAS EXTERNAS Y UTILIDADES ---

    def scan_local_filesystem(self, root_dir: str) -> Dict[str, Dict]:
        """
        Escanea el sistema de archivos local y devuelve un mapa de todos los archivos y directorios que existen físicamente.
        """
        local_files = {}
        
        try:
            for dirpath, dirnames, filenames in os.walk(root_dir):
                # Registrar directorio
                if dirpath != root_dir:
                    local_files[dirpath] = {
                        'type': 'dir',
                        'mtime': os.path.getmtime(dirpath),
                        'exists_on_disk': True
                    }
                
                # Registrar archivos
                for filename in filenames:
                    # Ignorar archivos temporales del sistema
                    if filename.endswith('.tmp') or '.delta.' in filename:
                        continue
                        
                    filepath = os.path.join(dirpath, filename)
                    try:
                        stat = os.stat(filepath)
                        local_files[filepath] = {
                            'type': 'file',
                            'size': stat.st_size,
                            'mtime': stat.st_mtime,
                            'exists_on_disk': True
                        }
                    except Exception as e:
                        print(f"[SCAN] Error al leer {filepath}: {e}")
                        
        except Exception as e:
            print(f"[SCAN] Error escaneando {root_dir}: {e}")
        
        return local_files

    def analyze_node_state(self, node_logs: List[Dict], node_ip: str, 
                      disk_scan: Dict[str, Dict] = None) -> Dict:
        """
        Nueva versión simplificada: Solo detecta archivos faltantes o desactualizados.
        No elimina nada del disco - solo reporta lo que debe ser sincronizado.
        """
        with self.lock:
            # 1. Reconstruir estado desde el log del nodo
            node_state_from_log = {}
            sorted_logs = sorted(node_logs, key=lambda x: x.get('timestamp', 0))
            
            for entry_data in sorted_logs:
                entry = LogEntry.from_dict(entry_data)
                if entry.op_type == 'STOR':
                    node_state_from_log[entry.path] = {'type': 'file', 'ts': entry.timestamp}
                elif entry.op_type == 'MKD':
                    node_state_from_log[entry.path] = {'type': 'dir', 'ts': entry.timestamp}
                elif entry.op_type in ['DELE', 'RMD']:
                    keys_to_remove = [k for k in node_state_from_log 
                                    if k == entry.path or k.startswith(entry.path + '/')]
                    for k in keys_to_remove: 
                        node_state_from_log.pop(k, None)
                elif entry.op_type == 'RN':
                    old = entry.metadata.get('old_path')
                    if old in node_state_from_log:
                        node_state_from_log[entry.path] = node_state_from_log[old]
                        del node_state_from_log[old]

            # 2. Si tenemos escaneo de disco, usar ESE como fuente de verdad
            node_state = disk_scan if disk_scan else node_state_from_log
            
            inconsistencies = []
            
            print(f"[ANALYZE] Analizando nodo {node_ip} con {len(node_state)} elementos en disco")
            
            # 3. DETECCIÓN DE FALTANTES Y DESACTUALIZADOS (solo detectar, no borrar)
            for path, global_info in self.file_map.items():
                if global_info.get('type') == 'file':
                    replicas = global_info.get('replicas', [])
                    
                    if node_ip in replicas:
                        if path not in node_state:
                            inconsistencies.append({
                                'command': 'CREATE_FILE',
                                'path': path,
                                'replicas': replicas,
                                'size': global_info.get('size', 0),
                                'hash': global_info.get('hash', ''),
                                'reason': 'missing_replica',
                                'priority': 2
                            })
                        else:
                            node_mtime = node_state[path].get('mtime', 0)
                            global_mtime = global_info.get('mtime', 0)
                            
                            if global_mtime > node_mtime + 5:
                                inconsistencies.append({
                                    'command': 'UPDATE_FILE',
                                    'path': path,
                                    'replicas': replicas,
                                    'size': global_info.get('size', 0),
                                    'hash': global_info.get('hash', ''),
                                    'reason': 'outdated_version',
                                    'global_mtime': global_mtime,
                                    'node_mtime': node_mtime,
                                    'priority': 3
                                })

            inconsistencies.sort(key=lambda x: x.get('priority', 99))

            return {
                'has_inconsistencies': len(inconsistencies) > 0,
                'inconsistencies': inconsistencies,
                'node_state_summary': {
                    'items_count': len(node_state),
                    'used_disk_scan': disk_scan is not None
                }
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
                print(f"[STATE] Directorio de usuario creado en estado global: {user_root}")

# Singleton Global
_state_manager_instance = None
def get_state_manager(node_id=None):
    global _state_manager_instance
    if _state_manager_instance is None:
        if not node_id: 
            node_id = os.environ.get('NODE_ID', 'unknown')
        _state_manager_instance = StateManager(node_id)
    return _state_manager_instance