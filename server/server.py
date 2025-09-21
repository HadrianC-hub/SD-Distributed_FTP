import socket
import threading
import os
import time

BUFFER_SIZE = 65536

def start_ftp_server():
    print('Minimal FTP server skeleton')

if __name__ == '__main__':
    start_ftp_server()

import socket
import threading
import os
import time
import platform
import random
import struct
import string
import json
import hashlib
import hmac

BUFFER_SIZE = 65536
# Límite de intentos fallidos y tiempo de bloqueo
MAX_FAILED_ATTEMPTS = 3
BLOCK_TIME = 300  # 5 minutos
INACTIVITY_TIMEOUT = 180  # 3 minutos

# Ajusta este directorio al lugar donde quieras que residan los datos en el host
SERVER_ROOT = os.environ.get('FTP_ROOT', '/server') # Busca la VARIABLE DE ENTORNO FTP_ROOT, o usa /server como Fallbck

HOST = "0.0.0.0"
PORT = 21

failed_attempts = {}

class Session:
