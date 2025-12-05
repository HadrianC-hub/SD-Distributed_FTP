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
        
    def discover_via_alias(self):
        """Descubre todos los contenedores con el alias ftp_cluster usando DNS de Docker"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                ips = socket.gethostbyname_ex(self.cluster_alias)[2]
                # Filtrar IPs vacías o inválidas
                valid_ips = [ip for ip in ips if ip and ip != '127.0.0.1']
                return set(valid_ips)
            
            except socket.gaierror as e:
                if "Name or service not known" not in str(e):
                    print(f"[ALIAS-DISCOVERY] Error resolviendo alias '{self.cluster_alias}' (intento {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    return set()
            except Exception as e:
                print(f"[ALIAS-DISCOVERY] Error inesperado resolviendo alias (intento {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    return set()
            
            # Esperar antes de reintentar
            if attempt < max_retries - 1:
                time.sleep(2)
        
        return set()

    def set_ips_change_callback(self, callback):
        """Establece un callback para cuando cambien las IPs"""
        self.ips_change_callback = callback
        
    def get_cluster_ips(self):
        """Retorna todas las IPs del cluster descubiertas"""
        with self.lock:
            return list(self.discovered_ips)
    
    def get_cluster_size(self):
        """Retorna el número de nodos en el cluster"""
        with self.lock:
            return len(self.discovered_ips)

    def _resolve_dns(self, name):
        try:
            return set(
                ip for ip in socket.gethostbyname_ex(name)[2]
                if ip and ip != "127.0.0.1"
            )
        except socket.gaierror:
            return set()

# Singleton global
_alias_discovery_instance = None
def start_alias_discovery(cluster_alias=None):
    """Inicia el descubrimiento por alias y retorna la instancia"""
    global _alias_discovery_instance
    if _alias_discovery_instance is None:
        _alias_discovery_instance = AliasServiceDiscovery(cluster_alias)
        _alias_discovery_instance.start_continuous_discovery(interval=15)
    return _alias_discovery_instance
