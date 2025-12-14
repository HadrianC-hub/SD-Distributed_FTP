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
        sorted_ips = sorted(list(set(all_ips)))
        
        with self.election_lock:
            old_ips = set(self.known_ips)
            new_ips = set(sorted_ips)
            added_ips = new_ips - old_ips
            removed_ips = old_ips - new_ips
            
            self.known_ips = sorted_ips
            
            if not added_ips and not removed_ips:
                return

            print(f"[BULLY] Cambio en topología detectado. Nodos: {sorted_ips}")
            
            if self.state == STATE_LEADER and added_ips:
                print(f"[BULLY] Soy líder. Dando la bienvenida a nuevos nodos: {added_ips}")
                self._send_coordinator_to_ips(list(added_ips))
            
            if self.leader_ip and self.leader_ip not in self.known_ips:
                print(f"[BULLY] Mi líder {self.leader_ip} ha desaparecido.")
                self.leader_ip = None
                self.state = STATE_FOLLOWER
                self._cancel_coordinator_timer()
            
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
        sender_ip = message.get('sender_ip')
        sender_leader = message.get('current_leader')
        
        print(f"[BULLY] Recibí ELECTION de {sender_ip}")
        
        if self.state == STATE_LEADER:
            print(f"[BULLY] Soy líder. Reenviando COORDINATOR a {sender_ip}")
            self._send_coordinator_to_ips([sender_ip])
            return {
                'status': 'alive', 
                'responder_ip': self.local_ip, 
                'is_leader': True,
                'my_leader': self.local_ip
            }
        
        if self.leader_ip and sender_leader and self.leader_ip != sender_leader:
            print(f"[BULLY] ¡CONFLICTO! Yo sigo a {self.leader_ip}, pero {sender_ip} sigue a {sender_leader}")
            print(f"[BULLY] Iniciando elección para resolver conflicto...")
            threading.Thread(target=self.start_election, daemon=True).start()
        
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
        leader_ip = message.get('leader_ip')
        leader_id = message.get('leader_id')
        cluster_ips = message.get('cluster_ips', [])
        
        print(f"[BULLY] Recibido COORDINATOR de {leader_ip} (ID: {leader_id})")
        
        self.reconstructing = False
        
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
        
        if self.leader_ip and self.leader_ip != leader_ip:
            if self.leader_ip not in cluster_ips:
                print(f"[BULLY] Mi líder anterior {self.leader_ip} no está vivo. Aceptando {leader_ip}.")
                with self.election_lock:
                    self.leader_ip = leader_ip
                    self.state = STATE_FOLLOWER
                    self._cancel_coordinator_timer()
                    print(f"[BULLY] Nuevo líder aceptado: {leader_ip}")
                    return {'status': 'ok', 'accepted_by': self.local_ip}
            else:
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
        
        with self.election_lock:
            self.leader_ip = leader_ip
            self.state = STATE_FOLLOWER
            print(f"[BULLY] Nuevo líder aceptado: {leader_ip}")
            
            if cluster_ips:
                self.known_ips = sorted(list(set(self.known_ips + cluster_ips)))
                print(f"[BULLY] Lista de nodos actualizada: {self.known_ips}")
            
            self._cancel_coordinator_timer()
        
        return {'status': 'ok', 'accepted_by': self.local_ip}

    # --- MANEJO DE ELECCIONES ---

    def start_election(self):
        with self.election_lock:
            if self.state == STATE_ELECTION:
                return
            self.state = STATE_ELECTION
            print(f"[BULLY] Iniciando proceso de elección...")
            self._cancel_coordinator_timer()
        
        time.sleep(random.uniform(0.1, 0.5))
        
        lower_ips = [ip for ip in self.known_ips if ip < self.local_ip]
        
        if not lower_ips:
            print("[BULLY] Soy la IP más baja disponible. Declaro victoria inmediatamente.")
            self._declare_victory()
            return

        print(f"[BULLY] Enviando mensajes ELECTION a nodos menores: {lower_ips}")
        
        election_results = []
        
        def challenge_node(target_ip):
            try:
                response = self.cluster_comm.send_message(
                    target_ip, 
                    {
                        'type': 'ELECTION', 
                        'sender_ip': self.local_ip,
                        'timestamp': time.time(),
                        'current_leader': self.leader_ip
                    },
                    expect_response=True
                )
                election_results.append((target_ip, response))
            except Exception as e:
                print(f"[BULLY] Nodo {target_ip} no respondió: {e}")
        
        threads = []
        for target_ip in lower_ips:
            t = threading.Thread(target=challenge_node, args=(target_ip,), daemon=True)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join(timeout=2.0)
        
        any_alive = False
        for target_ip, response in election_results:
            if response and response.get('status') == 'alive':
                print(f"[BULLY] Nodo menor {target_ip} respondió 'alive'.")
                any_alive = True
                break
        
        if any_alive:
            print("[BULLY] Nodos menores están vivos. Me retiro y espero COORDINATOR.")
            with self.election_lock:
                self.state = STATE_FOLLOWER
                self._schedule_coordinator_timeout()
        else:
            print("[BULLY] Ningún nodo menor respondió. Asumo liderazgo.")
            self._declare_victory()

    def _declare_victory(self):
        with self.election_lock:
            if self.state == STATE_LEADER:
                print(f"[BULLY] Ya soy líder, ignorando declaración de victoria.")
                return
                
            self.state = STATE_LEADER
            self.leader_ip = self.local_ip
            self.reconstructing = False
            print(f"[BULLY] ¡SOY EL LÍDER! ({self.local_ip})")
            self._cancel_coordinator_timer()
        
        msg = {
            'type': 'COORDINATOR',
            'leader_ip': self.local_ip,
            'leader_id': self.node_id,
            'timestamp': time.time(),
            'cluster_ips': self.known_ips
        }
        
        def send_coordinator():
            try:
                responses = self.cluster_comm.broadcast_message(msg, expect_responses=False)
                print(f"[BULLY] Mensaje COORDINATOR enviado a {len(responses)} nodos")
            except Exception as e:
                print(f"[BULLY] Error enviando COORDINATOR: {e}")
            
            self._on_leadership_gained()
        
        threading.Thread(target=send_coordinator, daemon=True).start()

    # --- MANEJO DE SINCRONIZACIÓN ---

    def _on_leadership_gained(self):
        if hasattr(self, '_leadership_gained_executed'):
            return
        self._leadership_gained_executed = True
        
        print("[LIDER] Iniciando reconstrucción del sistema de archivos...")
        threading.Thread(target=self._execute_log_sync, daemon=True).start()
        
        def schedule_forced_replication():
            time.sleep(10)
            if self.state == STATE_LEADER:
                from ftp.leader_operations import get_leader_operations
                ops = get_leader_operations(self.cluster_comm, self)
                if ops:
                    print("[BULLY] Programando replicación forzada post-reconstrucción...")
                    ops.check_replication_status()
        
        threading.Thread(target=schedule_forced_replication, daemon=True).start()

    def _execute_log_sync(self):
        if self.state == STATE_LEADER:
            self.reconstructing = True
            print("[BULLY] Líder entrando en modo reconstrucción")
        
        time.sleep(2)
        
        follower_ips = [ip for ip in self.known_ips if ip != self.local_ip]
        
        if not follower_ips:
            print("[LIDER-SYNC] No hay otros nodos.")
            if self.state == STATE_LEADER:
                self.reconstructing = False
            return
        
        ip_to_logs = {}
        
        leader_logs = self.state_manager.get_full_log_as_dict()
        ip_to_logs[self.local_ip] = leader_logs
        
        print(f"[LIDER-SYNC] Solicitando logs a {len(follower_ips)} seguidores...")
        
        for target_ip in follower_ips:
            try:
                response = self.cluster_comm.send_message(
                    target_ip, 
                    {
                        'type': 'REQUEST_LOGS', 
                        'leader_ip': self.local_ip,
                        'timestamp': time.time()
                    }
                )
                
                if response and response.get('status') == 'ok' and 'log' in response:
                    print(f"[LIDER-SYNC] Logs recibidos de {target_ip}. Tamaño: {len(response['log'])} entradas.")
                    ip_to_logs[target_ip] = response['log']
                else:
                    print(f"[LIDER-SYNC] Error o nodo sin log: {target_ip}")

            except Exception as e:
                print(f"[LIDER-SYNC] Error comunicando con {target_ip} para logs: {e}")
                if self.state == STATE_LEADER:
                    self.reconstructing = False

        print(f"[LIDER-SYNC] Fusionando {len(ip_to_logs)} fuentes...")
        
        all_logs_with_ips = []
        for node_ip, logs in ip_to_logs.items():
            all_logs_with_ips.append({
                'node_ip': node_ip,
                'log': logs
            })
        
        self.state_manager.merge_external_logs(all_logs_with_ips)
        
        print(f"[LIDER-SYNC] Analizando inconsistencias en {len(ip_to_logs)} nodos...")
        
        for node_ip, node_logs in ip_to_logs.items():
            if node_ip != self.local_ip:
                analysis = self.state_manager.analyze_node_state(node_logs, node_ip)
                
                if analysis['has_inconsistencies']:
                    print(f"[LIDER-SYNC] Nodo {node_ip} tiene {len(analysis['inconsistencies'])} inconsistencias")
                    
                    if analysis['inconsistencies']:
                        sync_msg = {
                            'type': 'SYNC_COMMANDS',
                            'commands': analysis['inconsistencies'],
                            'leader_ip': self.local_ip,
                            'timestamp': time.time()
                        }
                        
                        threading.Thread(
                            target=lambda ip, msg: self._send_sync_commands(ip, msg),
                            args=(node_ip, sync_msg),
                            daemon=True
                        ).start()
                else:
                    print(f"[LIDER-SYNC] Nodo {node_ip} está sincronizado")
        
        print("[LIDER-SYNC] Reconstrucción completada.")
        if self.state == STATE_LEADER:
            self.reconstructing = False
            print("[BULLY] Líder completó reconstrucción")
            
            self._check_and_repair_replication()

    def _check_and_repair_replication(self):
        try:
            from ftp.leader_operations import get_leader_operations
            ops = get_leader_operations(self.cluster_comm, self)
            if ops:
                print("[BULLY] Verificando estado de replicación tras reconstrucción...")
                ops.check_replication_status()
            else:
                print("[BULLY] No se pudo obtener LeaderOperations para verificar replicación.")
        except Exception as e:
            print(f"[BULLY] Error al verificar replicación: {e}")
            import traceback
            traceback.print_exc()

    def _send_sync_commands(self, target_ip: str, message: Dict):
        try:
            response = self.cluster_comm.send_message(target_ip, message, expect_response=True)
            if response and response.get('status') == 'ok':
                print(f"[LIDER-SYNC] Comandos de sincronización aceptados por {target_ip}")
            else:
                print(f"[LIDER-SYNC] Error en sincronización con {target_ip}: {response}")
        except Exception as e:
            print(f"[LIDER-SYNC] Error enviando comandos a {target_ip}: {e}")

    def _send_coordinator_to_ips(self, target_ips: List[str]):
        msg = {
            'type': 'COORDINATOR',
            'leader_ip': self.local_ip,
            'leader_id': self.node_id,
            'timestamp': time.time(),
            'cluster_ips': self.known_ips
        }
        
        for target_ip in target_ips:
            if target_ip != self.local_ip:
                print(f"[BULLY] Enviando COORDINATOR de bienvenida a {target_ip}")
                threading.Thread(
                    target=lambda ip: self.cluster_comm.send_message(ip, msg, expect_response=False),
                    args=(target_ip,),
                    daemon=True
                ).start()
        
        print(f"[BULLY] COORDINATOR enviado a {len(target_ips)} nodos. Sincronización en handle_node_join.")

    def _schedule_coordinator_timeout(self):
        self._cancel_coordinator_timer()
        self.coordinator_timer = threading.Timer(10.0, self._check_leader_timeout)
        self.coordinator_timer.daemon = True
        self.coordinator_timer.start()

    def _check_leader_timeout(self):
        with self.election_lock:
            if not self.leader_ip and self.state == STATE_FOLLOWER:
                print(f"[BULLY] Timeout esperando COORDINATOR. Iniciando elección.")
                self.start_election()

    def _cancel_coordinator_timer(self):
        if self.coordinator_timer:
            self.coordinator_timer.cancel()
            self.coordinator_timer = None

    # --- LLAMADAS EXTERNAS PARA COMPROBACIÓN DE ESTADO ---

    def is_reconstructing(self):
        with self.election_lock:
            return self.reconstructing

    def get_leader(self):
        with self.election_lock:
            return self.leader_ip

    def am_i_leader(self):
        with self.election_lock:
            return self.state == STATE_LEADER