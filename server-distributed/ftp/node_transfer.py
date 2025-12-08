import socket
import threading
import struct
import hashlib
import os
import time
import json
from typing import Tuple
from ftp.paths import calculate_file_hash, SERVER_ROOT


class NodeTransfer:
    NODE_DATA_PORT = 2125  # Puerto para transferencias entre nodos
    BUFFER_SIZE = 65536    # Tamaño del buffer para transferencias
    
    def __init__(self, local_ip: str):
        self.local_ip = local_ip
        self.transfer_socket = None
        self.active_transfers = {}
        self.transfer_lock = threading.Lock()
      