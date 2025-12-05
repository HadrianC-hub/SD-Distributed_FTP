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

