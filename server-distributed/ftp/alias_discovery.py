import socket
import time
import threading
import os

class AliasServiceDiscovery:
    def __init__(self, cluster_alias=None):
        self.cluster_alias = cluster_alias or os.environ.get('CLUSTER_ALIAS', 'ftp_cluster')
        self.discovered_ips = set()
        self.lock = threading.Lock()
        self.ips_change_callback = None
        self.last_logged_ips = set()  # Para evitar logs repetidos


