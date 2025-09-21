import socket
import os
import re
import struct

# Variables globales ----------------------------------------------------------------------------------------------------------

BUFFER_SIZE = 65536
TYPE = 'A'
MODE = 'S'
DATA_SOCKET = None                  # Socket de transferencia utilizado para transferencia de datos
DATA_SOCKET_IS_LISTENER = False


