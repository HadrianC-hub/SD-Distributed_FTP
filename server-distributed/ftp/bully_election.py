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

    # --- MANEJO DE ELECCIONES ---

    def start_election(self):
        """
        Lógica 'IP más baja gana'.
        """
        with self.election_lock:
            if self.state == STATE_ELECTION:
                return  # Ya en elección
            self.state = STATE_ELECTION
            print(f"[BULLY] Iniciando proceso de elección...")
            self._cancel_coordinator_timer()
        
        time.sleep(random.uniform(0.1, 0.5))  # Pequeño delay aleatorio
        
        # Obtener IPs de nodos con IP más baja
        lower_ips = [ip for ip in self.known_ips if ip < self.local_ip]
        
        # CASO 1: SOY LA IP MÁS BAJA
        if not lower_ips:
            print("[BULLY] Soy la IP más baja disponible. Declaro victoria inmediatamente.")
            self._declare_victory()
            return

        # CASO 2: Enviar ELECTION a nodos con IP más baja
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
        
        # Desafiar a todos los nodos menores en paralelo
        threads = []
        for target_ip in lower_ips:
            t = threading.Thread(target=challenge_node, args=(target_ip,), daemon=True)
            threads.append(t)
            t.start()
        
        # Esperar respuestas (tiempo máximo 2 segundos)
        for t in threads:
            t.join(timeout=2.0)
        
        # Evaluar respuestas
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
                self._schedule_coordinator_timeout()  # Programar timeout
        else:
            print("[BULLY] Ningún nodo menor respondió. Asumo liderazgo.")
            self._declare_victory()

    def _declare_victory(self):
        """Me convierto en Líder y notifico a todos"""
        with self.election_lock:
            if self.state == STATE_LEADER:
                print(f"[BULLY] Ya soy líder, ignorando declaración de victoria.")
                return
                
            self.state = STATE_LEADER
            self.leader_ip = self.local_ip
            self.reconstructing = False
            print(f"[BULLY] ¡SOY EL LÍDER! ({self.local_ip})")
            self._cancel_coordinator_timer()
        
        # Notificar a TODOS los nodos conocidos
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
            
            # Ejecutar acciones del líder
            self._on_leadership_gained()
        
        threading.Thread(target=send_coordinator, daemon=True).start()

    # --- MANEJO DE SINCRONIZACIÓN ---

    def _on_leadership_gained(self):
        """Acciones del líder tras ganar."""
        if hasattr(self, '_leadership_gained_executed'):
            return
        self._leadership_gained_executed = True
        
        print("[LIDER] Iniciando reconstrucción del sistema de archivos...")
        # Solo ejecutamos la reconstrucción inicial basada en logs (Critical)
        threading.Thread(target=self._execute_log_sync, daemon=True).start()
        
        # CORRECCIÓN: Programar replicación forzada después de la reconstrucción
        def schedule_forced_replication():
            time.sleep(10)  # Esperar a que termine la reconstrucción
            if self.state == STATE_LEADER:
                from ftp.leader_operations import get_leader_operations
                ops = get_leader_operations(self.cluster_comm, self)
                if ops:
                    print("[BULLY] Programando replicación forzada post-reconstrucción...")
                    # Forzar replicación completa si hay 2 nodos
                    ops.check_replication_status()
        
        threading.Thread(target=schedule_forced_replication, daemon=True).start()

    def _execute_log_sync(self):
        """
        Solicita logs a todos los nodos, fusiona y actualiza el mapa global.
        """
        # Solo marcar reconstructing si somos líder
        if self.state == STATE_LEADER:
            self.reconstructing = True
            print("[BULLY] Líder entrando en modo reconstrucción")
        
        time.sleep(2)  # Dar tiempo a que todos acepten el nuevo líder
        
        follower_ips = [ip for ip in self.known_ips if ip != self.local_ip]
        
        if not follower_ips:
            print("[LIDER-SYNC] No hay otros nodos.")
            if self.state == STATE_LEADER:
                self.reconstructing = False
            return
        
        # Mapeo de IP -> logs para análisis detallado
        ip_to_logs = {}
        
        # Recoger mi propio log primero
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

        # Fusionar todos los logs - CORRECCIÓN: Crear estructura correcta
        print(f"[LIDER-SYNC] Fusionando {len(ip_to_logs)} fuentes...")
        
        # Crear lista con estructura correcta para merge_external_logs
        all_logs_with_ips = []
        for node_ip, logs in ip_to_logs.items():
            all_logs_with_ips.append({
                'node_ip': node_ip,
                'log': logs
            })
        
        self.state_manager.merge_external_logs(all_logs_with_ips)
        
        # Detectar inconsistencias y ordenar sincronización
        print(f"[LIDER-SYNC] Analizando inconsistencias en {len(ip_to_logs)} nodos...")
        
        for node_ip, node_logs in ip_to_logs.items():
            if node_ip != self.local_ip:  # No comparar consigo mismo
                analysis = self.state_manager.analyze_node_state(node_logs, node_ip)
                
                if analysis['has_inconsistencies']:
                    print(f"[LIDER-SYNC] Nodo {node_ip} tiene {len(analysis['inconsistencies'])} inconsistencias")
                    
                    # Enviar comandos de sincronización
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
            
            # Auto-limpieza del líder DESACTIVADA durante sincronización inicial
            # Solo se ejecuta en GC periódico (cuando el sistema está estable)
            print("[BULLY] Auto-limpieza diferida al GC periódico (sistema en sincronización)")
            
            # Verificar y reparar réplicas insuficientes
            self._check_and_repair_replication()

    def _check_and_repair_replication(self):
        """Verifica y repara archivos con réplicas insuficientes."""
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
            traceback.print_exc()  # Para depuración

    def _send_sync_commands(self, target_ip: str, message: Dict):
        """Envía comandos de sincronización a un nodo"""
        try:
            response = self.cluster_comm.send_message(target_ip, message, expect_response=True)
            if response and response.get('status') == 'ok':
                print(f"[LIDER-SYNC] Comandos de sincronización aceptados por {target_ip}")
            else:
                print(f"[LIDER-SYNC] Error en sincronización con {target_ip}: {response}")
        except Exception as e:
            print(f"[LIDER-SYNC] Error enviando comandos a {target_ip}: {e}")

    def _send_coordinator_to_ips(self, target_ips: List[str]):
        """
        Envía COORDINATOR a IPs específicas.
        """
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
        """Programa el timeout para esperar COORDINATOR"""
        self._cancel_coordinator_timer()
        self.coordinator_timer = threading.Timer(10.0, self._check_leader_timeout)
        self.coordinator_timer.daemon = True
        self.coordinator_timer.start()

    def _check_leader_timeout(self):
        """Si no llegó COORDINATOR en el tiempo, iniciar elección"""
        with self.election_lock:
            if not self.leader_ip and self.state == STATE_FOLLOWER:
                print(f"[BULLY] Timeout esperando COORDINATOR. Iniciando elección.")
                self.start_election()

    def _cancel_coordinator_timer(self):
        """Cancela el timer de espera de COORDINATOR"""
        if self.coordinator_timer:
            self.coordinator_timer.cancel()
            self.coordinator_timer = None

    # --- LLAMADAS EXTERNAS PARA COMPROBACIÓN DE ESTADO ---

    def is_reconstructing(self):
        """Retorna si el líder está reconstruyendo."""
        with self.election_lock:
            return self.reconstructing

    def get_leader(self):
        with self.election_lock:
            return self.leader_ip

    def am_i_leader(self):
        with self.election_lock:
            return self.state == STATE_LEADER

    # --- MANEJO DE ARCHIVOS ZOMBIES ---

    def periodic_zombie_cleanup(self):
        """
        Ejecuta una limpieza periódica de zombies en todos los nodos.
        Esto detecta archivos huérfanos que puedan haber quedado.
        """
        if not self.am_i_leader():
            return
        
        print("[BULLY] Iniciando limpieza periódica de zombies...")
        
        for node_ip in self.known_ips:
            if node_ip == self.local_ip:
                continue
            
            try:
                # Solicitar logs y escaneo
                from ftp.sidecar import handle_node_join
                handle_node_join(node_ip)
            except Exception as e:
                print(f"[BULLY] Error en limpieza de {node_ip}: {e}")
        
        print("[BULLY] Limpieza periódica completada")
