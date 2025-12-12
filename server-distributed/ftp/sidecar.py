import os
import shutil
import threading
import time
from ftp.alias_discovery import start_alias_discovery
from ftp.cluster_comm import start_cluster_communication
from ftp.state_manager import get_state_manager
from ftp.bully_election import BullyElection
from ftp.leader_operations import process_local_leader_request, get_leader_operations
from ftp.node_transfer import get_node_transfer

# Variables globales
_cluster_comm_global = None
_bully_instance = None

def start_sidecar():
    global _cluster_comm_global, _bully_instance
    print(">>> Iniciando Sidecar...")
    node_id = os.environ.get('NODE_ID', 'unknown')
    
    discovery = start_alias_discovery()
    initial_ips = discovery.get_cluster_ips()
    _cluster_comm_global = start_cluster_communication(node_id, initial_ips)
    state_mgr = get_state_manager(node_id)

    # Iniciar Bully
    local_ip = _cluster_comm_global.local_ip
    _bully_instance = BullyElection(node_id, local_ip, _cluster_comm_global, state_mgr)
    
    # Registrar handlers
    _cluster_comm_global.register_handler('ELECTION', _bully_instance.handle_election_msg)
    _cluster_comm_global.register_handler('COORDINATOR', _bully_instance.handle_coordinator_msg)
    _cluster_comm_global.register_handler('REQUEST_LOGS', handle_request_logs)
    _cluster_comm_global.register_handler('SYNC_COMMANDS', handle_sync_commands)
    _cluster_comm_global.register_handler('FS_REQUEST', handle_fs_request)
    _cluster_comm_global.register_handler('FS_ORDER', handle_fs_order)
    _cluster_comm_global.register_handler('MKD_NOTIFY', handle_mkd_notify)
    _cluster_comm_global.register_handler('REPLICA_SUCCESS', handle_replica_success)
    _cluster_comm_global.register_handler('REQUEST_DISK_SCAN', handle_request_disk_scan)
    _cluster_comm_global.register_handler('DELTA_CONFIRMATION', handle_delta_confirmation)

    discovery.set_ips_change_callback(update_ips_callback)
    if initial_ips: update_ips_callback(initial_ips)
    
    get_node_transfer(local_ip) # Iniciar transfer server
    
    if _bully_instance.am_i_leader():
        get_leader_operations(_cluster_comm_global, _bully_instance)

    # Iniciar el verificador de replicación en un hilo separado
    threading.Thread(target=start_replication_checker, daemon=True).start()
    threading.Thread(target=start_periodic_zombie_cleanup, daemon=True).start()

    return _cluster_comm_global

