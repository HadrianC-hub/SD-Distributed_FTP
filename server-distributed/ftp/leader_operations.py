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
        
