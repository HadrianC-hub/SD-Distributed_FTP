import os
import time
from ftp.server_core import start_ftp_server, PORT, SERVER_ROOT
from ftp.sidecar import start_sidecar

if __name__ == "__main__":
    
    # Crear directorio data si no existe
    data_dir = os.path.join(os.path.abspath(SERVER_ROOT))
    os.makedirs(data_dir, exist_ok=True)

    # También crear la estructura de usuarios si no existe
    users_dir = os.path.join(data_dir, 'root')
    os.makedirs(users_dir, exist_ok=True)

    print("------------------------------------------------")
    node_id = os.environ.get('NODE_ID', 'unknown')
    print(f"--- Iniciando Nodo FTP Distribuido (ID: {node_id}) ---")
    print("------------------------------------------------")

    # 1. INICIAR SIDECAR
    cluster_comm = start_sidecar(node_id)
    time.sleep(5)

    try:
        # Solo en sistemas tipo Unix existe geteuid
        if hasattr(os, "geteuid") and os.geteuid() != 0 and PORT < 1024:
            print("Aviso: ejecutar sin root y puerto 21 fallará. Usando puerto 2121 para pruebas locales.")
            start_ftp_server(listen_port=2121)
        else:
            start_ftp_server()
    except AttributeError:
        # En Windows no hay geteuid: usar puerto alternativo si es <1024
        if PORT < 1024:
            print("Aviso: en Windows no se puede usar el puerto 21 sin privilegios. Usando 2121.")
            start_ftp_server(listen_port=2121)
        else:
            start_ftp_server()