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

    # --- HANDLERS DE MENSAJES ---

    def handle_election_msg(self, message):
        """Alguien me pregunta si estoy vivo"""
        sender_ip = message.get('sender_ip')
        sender_leader = message.get('current_leader')
        
        print(f"[BULLY] Recibí ELECTION de {sender_ip}")
        
        # Si ya soy líder, simplemente responder y reenviar COORDINATOR
        if self.state == STATE_LEADER:
            print(f"[BULLY] Soy líder. Reenviando COORDINATOR a {sender_ip}")
            self._send_coordinator_to_ips([sender_ip])
            return {
                'status': 'alive', 
                'responder_ip': self.local_ip, 
                'is_leader': True,
                'my_leader': self.local_ip
            }
        
        # Si tengo un líder diferente al del remitente, hay conflicto
        if self.leader_ip and sender_leader and self.leader_ip != sender_leader:
            print(f"[BULLY] ¡CONFLICTO! Yo sigo a {self.leader_ip}, pero {sender_ip} sigue a {sender_leader}")
            print(f"[BULLY] Iniciando elección para resolver conflicto...")
            threading.Thread(target=self.start_election, daemon=True).start()
        
        # Si tengo un líder y alguien con IP más alta me reta, debo iniciar elección
        elif self.leader_ip and sender_ip > self.local_ip:
            print(f"[BULLY] {sender_ip} (IP más alta) me reta. Iniciando elección.")
            threading.Thread(target=self.start_election, daemon=True).start()
        
        return {
            'status': 'alive', 
            'responder_ip': self.local_ip, 
            'is_leader': self.state == STATE_LEADER,
            'my_leader': self.leader_ip
        }

    def handle_coordinator_msg(self, message):
        """Alguien se declara líder"""
        leader_ip = message.get('leader_ip')
        leader_id = message.get('leader_id')
        cluster_ips = message.get('cluster_ips', [])
        
        print(f"[BULLY] Recibido COORDINATOR de {leader_ip} (ID: {leader_id})")
        
        # Limpiar estado de reconstrucción cuando seguimos a otro líder
        self.reconstructing = False
        
        # SI YO SOY EL LÍDER Y RECIBO COORDINATOR DE UN NODO CON IP MÁS BAJA
        if self.state == STATE_LEADER:
            if leader_ip < self.local_ip:
                print(f"[BULLY] Yo soy líder pero {leader_ip} tiene IP más baja ({self.local_ip}). Cediendo liderazgo.")
                with self.election_lock:
                    self.state = STATE_FOLLOWER
                    self.leader_ip = leader_ip
                    print(f"[BULLY] Ahora sigo a {leader_ip} como líder")
                    self._cancel_coordinator_timer()
                return {'status': 'ok', 'accepted_by': self.local_ip}
            else:
                print(f"[BULLY] Yo ya soy líder. Ignorando COORDINATOR de {leader_ip}")
                return {'status': 'ignored', 'reason': 'already_leader'}
        
        # Si ya tengo un líder diferente
        if self.leader_ip and self.leader_ip != leader_ip:
            # Verificar si mi líder actual está vivo (en la lista de cluster_ips)
            if self.leader_ip not in cluster_ips:
                # Mi líder anterior no está vivo, aceptar al nuevo
                print(f"[BULLY] Mi líder anterior {self.leader_ip} no está vivo. Aceptando {leader_ip}.")
                with self.election_lock:
                    self.leader_ip = leader_ip
                    self.state = STATE_FOLLOWER
                    self._cancel_coordinator_timer()
                    print(f"[BULLY] Nuevo líder aceptado: {leader_ip}")
                    return {'status': 'ok', 'accepted_by': self.local_ip}
            else:
                # AMBOS líderes están vivos → usar "IP más baja gana"
                if leader_ip < self.leader_ip:
                    print(f"[BULLY] {leader_ip} es IP más baja que {self.leader_ip}. Cambiando de líder.")
                    with self.election_lock:
                        self.leader_ip = leader_ip
                        self.state = STATE_FOLLOWER
                        self._cancel_coordinator_timer()
                        return {'status': 'ok', 'accepted_by': self.local_ip}
                else:
                    print(f"[BULLY] Rechazando COORDINATOR de {leader_ip} (mi líder {self.leader_ip} tiene IP más baja)")
                    return {'status': 'rejected', 'reason': 'higher_ip_leader_exists'}
        
        # Si no tengo líder o es el mismo, aceptar
        with self.election_lock:
            self.leader_ip = leader_ip
            self.state = STATE_FOLLOWER
            print(f"[BULLY] Nuevo líder aceptado: {leader_ip}")
            
            # Actualizar lista de IPs
            if cluster_ips:
                self.known_ips = sorted(list(set(self.known_ips + cluster_ips)))
                print(f"[BULLY] Lista de nodos actualizada: {self.known_ips}")
            
            self._cancel_coordinator_timer()
        
        return {'status': 'ok', 'accepted_by': self.local_ip}
