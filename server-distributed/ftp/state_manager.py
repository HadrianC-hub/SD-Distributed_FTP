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

