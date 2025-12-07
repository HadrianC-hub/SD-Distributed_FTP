import socket
import threading
import json
import time
import uuid
from typing import Dict, List, Callable
from json import JSONDecoder

class ClusterCommunication:
    def __init__(self, node_id: str, cluster_ips: List[str]):
        self.node_id = node_id
        self.cluster_ips = cluster_ips
        self.message_handlers = {}
        self.server_socket = None
        self.pending_operations = {} 
        self.operation_timeout = 10
        self.local_ip = self.get_local_ip()
        print(f"[CLUSTER] IP local detectada: {self.local_ip} para el nodo {node_id}")
        
    def get_local_ip(self):
        """Obtiene la IP local del contenedor"""
        try:
            # En Docker, el hostname es la IP del contenedor
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            return local_ip
        except Exception:
            try:
                # Fallback: método alternativo
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                local_ip = s.getsockname()[0]
                s.close()
                return local_ip
            except Exception:
                return '127.0.0.1'
  
