import os
import threading
import time
import random
from typing import List, Optional, Dict

# Estados del nodo
STATE_FOLLOWER = 'FOLLOWER'
STATE_ELECTION = 'ELECTION'
STATE_LEADER = 'LEADER'

class BullyElection:
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
        self.reconstruction_cancelled = threading.Event()

    @staticmethod
    def ip_to_tuple(ip: str) -> tuple:
        """Convierte IP a tupla de enteros para comparación correcta"""
        try:
            return tuple(int(part) for part in ip.split('.'))
        except:
            return (0, 0, 0, 0)

    def compare_ips(ip1: str, ip2: str) -> int:
        """Compara dos IPs. Retorna -1 si ip1 < ip2, 0 si iguales, 1 si ip1 > ip2"""
        tuple1 = BullyElection.ip_to_tuple(ip1)
        tuple2 = BullyElection.ip_to_tuple(ip2)
        if tuple1 < tuple2:
            return -1
        elif tuple1 > tuple2:
            return 1
        return 0

    # --- FUNCIÓN DE ACTUALIZACIÓN DE ESTADO ---

    def update_nodes(self, all_ips: List[str]):
        sorted_ips = sorted(list(set(all_ips)), key=BullyElection.ip_to_tuple)
        
        with self.election_lock:
            old_ips = set(self.known_ips)
            new_ips = set(sorted_ips)
            added_ips = new_ips - old_ips
            removed_ips = old_ips - new_ips
            
            self.known_ips = sorted_ips
            
            if not added_ips and not removed_ips:
                return

            print(f"[BULLY] Cambio en topología detectado. Nodos: {sorted_ips}")
            
            # Si soy líder y detecto un nodo con IP más baja, CANCELAR TODO
            if self.state == STATE_LEADER and added_ips:
                lowest_ip = sorted_ips[0]
                
                if BullyElection.compare_ips(lowest_ip, self.local_ip) < 0:
                    print(f"[BULLY] LÍDER CON IP MÁS BAJA DETECTADO: {lowest_ip}")
                    print(f"[BULLY] CANCELANDO RECONSTRUCCIÓN Y CEDIENDO LIDERAZGO")
                    
                    with self.election_lock:
                        # Marcar reconstrucción como cancelada
                        self.reconstruction_cancelled.set()
                        self.reconstructing = False
                        
                        # Ceder liderazgo
                        self.state = STATE_FOLLOWER
                        self.leader_ip = None
                        self._cancel_coordinator_timer()
                    
                    print(f"[BULLY] Esperando COORDINATOR de {lowest_ip}...")
                    self._schedule_coordinator_timeout()
                    return
                
                # Si hay nodos nuevos pero yo soy el de IP más baja, dar bienvenida
                print(f"[BULLY] Soy líder con IP más baja. Dando bienvenida a: {added_ips}")
                self._send_coordinator_to_ips(list(added_ips))
            
            # Si mi líder desapareció
            if self.leader_ip and self.leader_ip not in self.known_ips:
                print(f"[BULLY] Mi líder {self.leader_ip} ha desaparecido.")
                self.leader_ip = None
                self.state = STATE_FOLLOWER
                self._cancel_coordinator_timer()
            
            # Si no tengo líder, esperar o iniciar elección
            if not self.leader_ip:
                lowest_ip = sorted_ips[0] if sorted_ips else None
                
                if BullyElection.compare_ips(self.local_ip, lowest_ip) == 0:
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
        
        # Si estoy reconstruyendo y llega un COORDINATOR de IP más baja, CANCELAR
        if self.reconstructing:
            if BullyElection.compare_ips(leader_ip, self.local_ip) < 0:
                print(f"[BULLY] Cancelando reconstrucción: líder verdadero es {leader_ip}")
                self.reconstruction_cancelled.set()
                self.reconstructing = False
        
        if self.state == STATE_LEADER:
            if BullyElection.compare_ips(leader_ip, self.local_ip) < 0:
                print(f"[BULLY] Yo soy líder pero {leader_ip} tiene IP más baja. Cediendo liderazgo.")
                with self.election_lock:
                    self.state = STATE_FOLLOWER
                    self.leader_ip = leader_ip
                    print(f"[BULLY] Ahora sigo a {leader_ip} como líder")
                    self._cancel_coordinator_timer()
                return {'status': 'ok', 'accepted_by': self.local_ip}
            elif BullyElection.compare_ips(leader_ip, self.local_ip) > 0:
                print(f"[BULLY] Yo soy líder con IP más baja. Rechazando COORDINATOR de {leader_ip}")
                threading.Thread(target=self._send_coordinator_to_ips, args=([leader_ip],), daemon=True).start()
                return {'status': 'rejected', 'reason': 'lower_ip_leader_exists', 'true_leader': self.local_ip}
            else:
                print(f"[BULLY] Conflicto: Mismo IP declarando liderazgo???")
                return {'status': 'error', 'reason': 'ip_conflict'}
        
        if not self.leader_ip or BullyElection.compare_ips(leader_ip, self.leader_ip) < 0:
            with self.election_lock:
                self.leader_ip = leader_ip
                self.state = STATE_FOLLOWER
                print(f"[BULLY] Nuevo líder aceptado: {leader_ip}")
                
                if cluster_ips:
                    self.known_ips = sorted(list(set(self.known_ips + cluster_ips)), key=BullyElection.ip_to_tuple)
                    print(f"[BULLY] Lista de nodos actualizada: {self.known_ips}")
                
                self._cancel_coordinator_timer()
            
            return {'status': 'ok', 'accepted_by': self.local_ip}
        
        print(f"[BULLY] Rechazando COORDINATOR de {leader_ip} (mi líder {self.leader_ip} tiene IP más baja)")
        return {'status': 'rejected', 'reason': 'lower_ip_leader_exists', 'current_leader': self.leader_ip}

    # --- MANEJO DE ELECCIONES ---

    def start_election(self):
        with self.election_lock:
            if self.state == STATE_ELECTION:
                return
            self.state = STATE_ELECTION
            print(f"[BULLY] Iniciando proceso de elección...")
            self._cancel_coordinator_timer()
        
        time.sleep(random.uniform(0.1, 0.5))
        
        lower_ips = [ip for ip in self.known_ips if BullyElection.compare_ips(ip, self.local_ip) < 0]
        
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
            self.reconstruction_cancelled.clear()
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
        """
        Sincronización completa con manejo de conflictos por HASH. SIEMPRE solicita disk_scan para obtener hashes reales.
        """
        self.reconstruction_cancelled.clear()
        
        if self.state != STATE_LEADER:
            return
            
        self.reconstructing = True
        print("[BULLY] Líder entrando en modo reconstrucción")
        
        try:
            # 1. ESTABILIZACIÓN
            print("[LIDER-SYNC] Esperando estabilización del cluster...")
            for i in range(5):
                time.sleep(1)
                if self.reconstruction_cancelled.is_set():
                    print("[LIDER-SYNC] Reconstrucción cancelada")
                    return
                if self.state != STATE_LEADER:
                    print("[LIDER-SYNC] Ya no soy líder")
                    return
            
            print("[LIDER-SYNC] === INICIO DE SINCRONIZACIÓN ===")
            follower_ips = [ip for ip in self.known_ips if ip != self.local_ip]
            
            # 2. RECOLECCIÓN DE LOGS Y DISK SCANS (OBLIGATORIO)
            all_logs_with_ips = []
            
            # PASO 1: Obtener logs y scan del LÍDER PRIMERO
            from ftp.paths import SERVER_ROOT
            leader_root = os.path.join(SERVER_ROOT, 'root')
            
            print(f"[LIDER-SYNC] Escaneando disco local: {leader_root}")
            leader_scan = self.state_manager.scan_local_filesystem(leader_root)
            leader_logs = self.state_manager.get_full_log_as_dict()
            
            print(f"[LIDER-SYNC] Líder ({self.local_ip}): {len(leader_logs)} ops, {len(leader_scan)} archivos escaneados")
            
            # Agregar líder PRIMERO con scan completo
            all_logs_with_ips.append({
                'node_ip': self.local_ip,
                'log': leader_logs,
                'scan': leader_scan
            })
            
            # PASO 2: Si no hay seguidores, merge solo con líder
            if not follower_ips:
                print("[LIDER-SYNC] No hay otros nodos, aplicando estado del líder...")
                # Aún así hacer merge para limpiar estado
                _, conflict_actions = self.state_manager.merge_external_logs(all_logs_with_ips)
                print(f"[LIDER-SYNC] === RECONSTRUCCIÓN COMPLETADA (solo líder) ===")
                return
            
            # PASO 3: Solicitar logs y scans de seguidores
            print(f"[LIDER-SYNC] Solicitando logs y escaneos a {len(follower_ips)} seguidores...")
            
            for target_ip in follower_ips:
                if self.reconstruction_cancelled.is_set():
                    return
                
                try:
                    # Solicitar logs
                    print(f"[LIDER-SYNC] Solicitando logs a {target_ip}...")
                    log_response = self.cluster_comm.send_message(
                        target_ip, 
                        {'type': 'REQUEST_LOGS', 'leader_ip': self.local_ip},
                        expect_response=True
                    )
                    
                    node_log = []
                    if log_response and log_response.get('status') == 'ok':
                        node_log = log_response.get('log', [])
                        print(f"[LIDER-SYNC] Logs recibidos de {target_ip}: {len(node_log)} operaciones")
                    else:
                        print(f"[LIDER-SYNC] Error obteniendo logs de {target_ip}")
                    
                    # Solicitar escaneo de disco (OBLIGATORIO para detección de conflictos)
                    print(f"[LIDER-SYNC] Solicitando escaneo de disco a {target_ip}...")
                    scan_response = self.cluster_comm.send_message(
                        target_ip,
                        {
                            'type': 'REQUEST_DISK_SCAN',
                            'root_path': leader_root  # Usar misma ruta que el líder
                        },
                        expect_response=True
                    )
                    
                    node_scan = {}
                    if scan_response and scan_response.get('status') == 'ok':
                        node_scan = scan_response.get('disk_scan', {})
                        print(f"[LIDER-SYNC] Escaneo recibido de {target_ip}: {len(node_scan)} elementos")
                    else:
                        print(f"[LIDER-SYNC] Error obteniendo escaneo de {target_ip}")
                        # Continuar aunque no tengamos scan - el merge manejará esto
                    
                    # Agregar datos del nodo
                    all_logs_with_ips.append({
                        'node_ip': target_ip,
                        'log': node_log,
                        'scan': node_scan
                    })
                            
                except Exception as e:
                    print(f"[LIDER-SYNC] Error con {target_ip}: {e}")
                    # Agregar entrada vacía para mantener el nodo en el merge
                    all_logs_with_ips.append({
                        'node_ip': target_ip,
                        'log': [],
                        'scan': {}
                    })
            
            if self.reconstruction_cancelled.is_set():
                return
            
            # 3. FUSIÓN CON DETECCIÓN DE CONFLICTOS
            print(f"[LIDER-SYNC] === INICIANDO FUSIÓN ===")
            print(f"[LIDER-SYNC] Datos recolectados de {len(all_logs_with_ips)} nodos:")
            for node_data in all_logs_with_ips:
                node_ip = node_data['node_ip']
                log_count = len(node_data.get('log', []))
                scan_count = len(node_data.get('scan', {}))
                print(f"[LIDER-SYNC]   - {node_ip}: {log_count} ops, {scan_count} archivos")
            
            cleanup_commands, conflict_actions = self.state_manager.merge_external_logs(all_logs_with_ips)
            
            if self.reconstruction_cancelled.is_set():
                return
            
            # 4. EJECUTAR ACCIONES DE CONFLICTO (por prioridad)
            if conflict_actions:
                print(f"[LIDER-SYNC] Ejecutando {len(conflict_actions)} acciones de resolución...")
                
                local_ip = self.cluster_comm.local_ip
                
                # Ordenar por prioridad
                conflict_actions.sort(key=lambda x: x.get('priority', 99))
                
                # FASE 1: Renombrar archivos locales (priority=1)
                rename_actions = [a for a in conflict_actions if a.get('priority') == 1]
                if rename_actions:
                    print(f"[LIDER-SYNC] FASE 1: Renombrando {len(rename_actions)} archivos...")
                    
                    for action in rename_actions:
                        node_ip = action['node']
                        old_path = action['old_path']
                        new_path = action['new_path']
                        expected_hash = action['expected_hash']
                        version = action.get('version', 0)
                        
                        if node_ip == local_ip:
                            # Renombrar localmente
                            try:
                                if os.path.exists(old_path):
                                    from ftp.paths import calculate_file_hash
                                    actual_hash = calculate_file_hash(old_path)
                                    
                                    if actual_hash == expected_hash:
                                        os.makedirs(os.path.dirname(new_path), exist_ok=True)
                                        os.rename(old_path, new_path)
                                        print(f"[LIDER-SYNC] Local rename: {old_path} -> {new_path}")
                                        
                                        self.state_manager.append_operation('RN', new_path, {
                                            'old_path': old_path,
                                            'from_conflict': True,
                                            'version': version
                                        })
                                    else:
                                        print(f"[LIDER-SYNC] Hash no coincide para {old_path}")
                            except Exception as e:
                                print(f"[LIDER-SYNC] Error renombrando {old_path}: {e}")
                        else:
                            # Enviar comando a nodo remoto
                            try:
                                self.cluster_comm.send_message(node_ip, {
                                    'type': 'SYNC_COMMANDS',
                                    'commands': [{
                                        'command': 'RENAME_LOCAL_TO_VERSION',
                                        'old_path': old_path,
                                        'new_path': new_path,
                                        'expected_hash': expected_hash,
                                        'expected_size': action.get('expected_size', 0),
                                        'version': version
                                    }],
                                    'phase': 'conflict_resolution_phase1'
                                }, expect_response=False)
                                print(f"[LIDER-SYNC] Comando enviado a {node_ip}")
                            except Exception as e:
                                print(f"[LIDER-SYNC] Error enviando a {node_ip}: {e}")
                    
                    print(f"[LIDER-SYNC] Esperando 3s...")
                    time.sleep(3)
                
                # FASE 2: Replicar versiones (priority=2)
                replicate_actions = [a for a in conflict_actions if a.get('priority') == 2]
                if replicate_actions:
                    print(f"[LIDER-SYNC] FASE 2: Replicando {len(replicate_actions)} versiones...")
                    
                    for action in replicate_actions:
                        source_node = action['source_node']
                        target_node = action['target_node']
                        path = action['path']
                        expected_hash = action['expected_hash']
                        
                        print(f"[LIDER-SYNC] Replicando {path} de {source_node} a {target_node}")
                        
                        message = {
                            'type': 'FS_ORDER',
                            'command': 'REPLICATE_FILE',
                            'path': path,
                            'source': source_node,
                            'size': action.get('expected_size', 0),
                            'hash': expected_hash,
                            'operation_id': f"version_repl_{int(time.time())}",
                            'timestamp': time.time(),
                            'is_version_replication': True,
                            'version': action.get('version', 0)
                        }
                        
                        try:
                            self.cluster_comm.send_message(target_node, message, expect_response=False)
                            print(f"[LIDER-SYNC] Replicación enviada a {target_node}")
                        except Exception as e:
                            print(f"[LIDER-SYNC] Error: {e}")
                    
                    print(f"[LIDER-SYNC] Esperando 5s para replicaciones...")
                    time.sleep(5)
                
                # FASE 3: Borrar originales (priority=3)
                delete_actions = [a for a in conflict_actions if a.get('priority') == 3]
                if delete_actions:
                    print(f"[LIDER-SYNC] FASE 3: Borrando {len(delete_actions)} archivos originales...")
                    
                    for action in delete_actions:
                        node_ip = action['node']
                        path = action['path']
                        
                        if node_ip == local_ip:
                            # Borrar localmente
                            try:
                                if os.path.exists(path):
                                    os.remove(path)
                                    print(f"[LIDER-SYNC] Local delete: {path}")
                                    
                                    self.state_manager.append_operation('DELE', path, {
                                        'from_conflict': True,
                                        'reason': 'original_removed_after_versioning'
                                    })
                            except Exception as e:
                                print(f"[LIDER-SYNC] Error borrando {path}: {e}")
                        else:
                            # Enviar comando a nodo remoto
                            try:
                                self.cluster_comm.send_message(node_ip, {
                                    'type': 'SYNC_COMMANDS',
                                    'commands': [{
                                        'command': 'DELETE_ORIGINAL',
                                        'path': path,
                                        'reason': action.get('reason', 'conflict_cleanup')
                                    }],
                                    'phase': 'conflict_resolution_phase3'
                                }, expect_response=False)
                                print(f"[LIDER-SYNC] Delete enviado a {node_ip}")
                            except Exception as e:
                                print(f"[LIDER-SYNC] Error enviando a {node_ip}: {e}")
                    
                    print(f"[LIDER-SYNC] Esperando 2s...")
                    time.sleep(2)
            
            print("[LIDER-SYNC] === RECONSTRUCCIÓN COMPLETADA ===")
            
        except Exception as e:
            print(f"[LIDER-SYNC] ERROR: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            if self.state == STATE_LEADER and not self.reconstruction_cancelled.is_set():
                self.reconstructing = False
                print("[BULLY] Líder salió del modo reconstrucción")
                try:
                    self._check_and_repair_replication()
                except Exception as e:
                    print(f"[LIDER-SYNC] Error en reparación: {e}")
            else:
                self.reconstructing = False

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
        
        print(f"[BULLY] COORDINATOR enviado a {len(target_ips)} nodos.")

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

    def get_leader(self):
        with self.election_lock:
            return self.leader_ip

    def am_i_leader(self):
        with self.election_lock:
            return self.state == STATE_LEADER