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

    def start_continuous_discovery(self, interval=10):
        """Inicia descubrimiento continuo en segundo plano"""

        def discovery_loop():
            attempt = 0
            initial_phase = True    

            while True:
                new_ips = self.discover_via_alias()

                with self.lock:
                    # Detectar cambios
                    added = new_ips - self.discovered_ips
                    removed = self.discovered_ips - new_ips

                    # Solo loggear si hay cambios o es el primer descubrimiento
                    should_log = added or removed or attempt == 0

                    if should_log:
                        print(f"[ALIAS-DISCOVERY] Estado del cluster: {len(new_ips)} nodos -> {sorted(new_ips)}")
                    if added:
                        print(f"[ALIAS-DISCOVERY] Nodos añadidos: {added}")
                    if removed:
                        print(f"[ALIAS-DISCOVERY] Nodos removidos: {removed}")

                    # Actualizar último estado loggeado
                    if should_log:
                        self.last_logged_ips = new_ips.copy()

                    self.discovered_ips = new_ips

                # Llamar al callback si hay cambios y está configurado
                if self.ips_change_callback and (added or removed or attempt == 0):
                    try:
                        self.ips_change_callback(list(new_ips))
                    except Exception as e:
                        print(f"[ALIAS-DISCOVERY] Error en callback: {e}")

                attempt += 1

                # Fase inicial más agresiva (primeros 60 segundos)
                if initial_phase:
                    if attempt <= 12:  # 12 * 5s = 60 segundos
                        sleep_time = 5
                    else:
                        initial_phase = False
                        sleep_time = interval
                else:
                    sleep_time = interval

                time.sleep(sleep_time)
        
        thread = threading.Thread(target=discovery_loop, daemon=True)
        thread.start()
        return thread
        

