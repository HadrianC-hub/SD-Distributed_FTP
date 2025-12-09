import threading
import time
import random
from typing import List, Optional, Dict

# Estados del nodo
STATE_FOLLOWER = 'FOLLOWER'
STATE_ELECTION = 'ELECTION'
STATE_LEADER = 'LEADER'

class BullyElection:
    ELECTION_TIMEOUT = 4.0

    def __init__(self, node_id: str, local_ip: str, cluster_comm, state_manager):
        self.node_id = node_id
        self.local_ip = local_ip
        self.cluster_comm = cluster_comm
        self.state_manager = state_manager
        self.state = STATE_FOLLOWER
        self.leader_ip: Optional[str] = None
        self.election_lock = threading.RLock() 
        self.known_ips: List[str] = []
        self.coordinator_timer = None
        self.reconstructing = False

    # --- FUNCIÓN DE ACTUALIZACIÓN DE ESTADO ---

    def update_nodes(self, all_ips: List[str]):
        """
        Llamado por el Sidecar cuando el Discovery detecta cambios.
        """
        # Limpiar y ordenar IPs
        sorted_ips = sorted(list(set(all_ips)))
        
        with self.election_lock:
            # Guardar IPs anteriores para detectar cambios
            old_ips = set(self.known_ips)
            new_ips = set(sorted_ips)
            added_ips = new_ips - old_ips
            removed_ips = old_ips - new_ips
            
            # Actualizar lista de IPs conocidas
            self.known_ips = sorted_ips
            
            # Si no hay cambios en las IPs, no hacer nada
            if not added_ips and not removed_ips:
                return

            print(f"[BULLY] Cambio en topología detectado. Nodos: {sorted_ips}")
            if added_ips:
                print(f"[BULLY] Nodos añadidos: {added_ips}")
            if removed_ips:
                print(f"[BULLY] Nodos removidos: {removed_ips}")
            
            # Si soy el líder, dar la bienvenida a nuevos nodos
            if self.state == STATE_LEADER and added_ips:
                print(f"[BULLY] Soy líder. Dando la bienvenida a nuevos nodos: {added_ips}")
                self._send_coordinator_to_ips(list(added_ips))
            
            # Si perdí mi líder o nunca tuve uno
            if self.leader_ip and self.leader_ip not in self.known_ips:
                print(f"[BULLY] Mi líder {self.leader_ip} ha desaparecido.")
                self.leader_ip = None
                self.state = STATE_FOLLOWER
                self._cancel_coordinator_timer()
            
            # Si no tengo líder, determinar quién debería ser
            if not self.leader_ip:
                lowest_ip = sorted_ips[0] if sorted_ips else None
                
                if self.local_ip == lowest_ip:
                    print(f"[BULLY] Soy la IP más baja ({self.local_ip}). Inicio la elección.")
                    threading.Thread(target=self.start_election, daemon=True).start()
                else:
                    print(f"[BULLY] No tengo líder. Esperando COORDINATOR...")
                    self._schedule_coordinator_timeout()
