import streamlit as st
import os
import sys
import io
import time
import re
import time
import client as client

st.set_page_config(page_title="Cliente FTP", page_icon="📂", layout="wide")


# -----------------------------------------------------------------------------------------------------
# Estado inicial

# -----------------------------------------------------------------------------------------------------
if "console" not in st.session_state:
    st.session_state.console = []
if "show_console" not in st.session_state:
    st.session_state.show_console = False
if "ftp_client" not in st.session_state:
    st.session_state.ftp_client = None
if "current_dir" not in st.session_state:
    st.session_state.current_dir = "/"
if "navigation_lock" not in st.session_state:
    st.session_state.navigation_lock = False
if "connection_id" not in st.session_state:
    st.session_state.connection_id = None
if "delete_candidate" not in st.session_state:
    st.session_state.delete_candidate = None  # (name, type)
if "creating_folder" not in st.session_state:
    st.session_state.creating_folder = False
if "new_folder_name" not in st.session_state:
    st.session_state.new_folder_name = ""
if "download_candidate" not in st.session_state:
    st.session_state.download_candidate = None  # (name, type)
if "download_path" not in st.session_state:
    st.session_state.download_path = "downloads"
if "transfer_mode" not in st.session_state:
    st.session_state.transfer_mode = 'S'  # Por defecto Stream
if "transfer_type" not in st.session_state:
    st.session_state.transfer_type = 'A'  # Por defecto ASCII
if "renaming_candidate" not in st.session_state:
    st.session_state.renaming_candidate = None  # (name, type)
if "new_name" not in st.session_state:
    st.session_state.new_name = ""
if "download_port_candidate" not in st.session_state:
    st.session_state.download_port_candidate = None  # (name, type)
if "using_port_mode" not in st.session_state:
    st.session_state.using_port_mode = False
if "upload_candidate" not in st.session_state:
    st.session_state.upload_candidate = None
if "upload_path" not in st.session_state:
    st.session_state.upload_path = ""
if "uploading" not in st.session_state:
    st.session_state.uploading = False
if "stou_upload_candidate" not in st.session_state:
    st.session_state.stou_upload_candidate = None
if "stou_upload_path" not in st.session_state:
    st.session_state.stou_upload_path = ""
if "stou_uploading" not in st.session_state:
    st.session_state.stou_uploading = False
if "stou_multiple_files" not in st.session_state:
    st.session_state.stou_multiple_files = []
if "append_candidate" not in st.session_state:
    st.session_state.append_candidate = None  # (name, type)
if "append_local_path" not in st.session_state:
    st.session_state.append_local_path = ""
if "appending" not in st.session_state:
    st.session_state.appending = False
if "_need_rerun" not in st.session_state:
    st.session_state["_need_rerun"] = False
if "_last_rerun_time" not in st.session_state:
    st.session_state["_last_rerun_time"] = 0.0


# -----------------------------------------------------------------------------------------------------
# Funciones auxiliares

# -----------------------------------------------------------------------------------------------------

# --- FUNCIONES PARA RECARGAR PÁGINA SIN ROMPERLA (Comptibilidad con Docker) ---

# Cooldown por acción (evita ejecuciones repetidas en corto tiempo)
def can_do_action(name: str, cooldown: float = 1.0) -> bool:
    now = time.time()
    key = f"_last_action_{name}"
    last = st.session_state.get(key, 0.0)
    if now - last < cooldown:
        # demasiado pronto para repetir
        return False
    st.session_state[key] = now
    return True

# En lugar de llamar request_rerun() directamente, llamar a request_rerun()
def request_rerun(cooldown: float = 0.5):
    st.session_state["_need_rerun"] = True
    st.session_state["_requested_rerun_cooldown"] = cooldown

# --- FUNCIONES PARA APPEND ---

def start_append(item_name, item_type):
    """Inicia el proceso de append."""
    st.session_state.append_candidate = (item_name, item_type)
    st.session_state.append_local_path = ""

def confirm_and_append():
    """Confirma y ejecuta el append."""

    # Verificando que exista candidato a APPEND
    if not st.session_state.append_candidate:
        return
    
    # Cargando candidato a APPEND y origen
    remote_name, _ = st.session_state.append_candidate
    local_path = st.session_state.append_local_path.strip()
    
    # Tirando errores a la interfaz en caso de que el origen no sea válido
    if not local_path:
        st.error("❌ La ruta local no puede estar vacía")
        return
    if not os.path.exists(local_path):
        st.error("❌ La ruta especificada no existe")
        return
    if os.path.isdir(local_path):
        st.error("❌ No se puede hacer append de un directorio")
        return
    
    # Haciendo APPEND
    success, message = append_to_server(st.session_state.ftp_client, local_path, remote_name)
    
    # Mostrando mensajes
    if success:
        st.success(message)
        log_message(f"📝 Append exitoso: {local_path} -> {remote_name}")
    else:
        log_message(f"💥 Error en append: {local_path} -> {remote_name}")
        st.error(message)
    
    # Limpiando el estado de append
    st.session_state.append_candidate = None
    st.session_state.append_local_path = ""
    st.session_state.appending = False

    # Solicitud de recarga
    request_rerun()

def cancel_append():
    """Cancela el proceso de append."""
    st.session_state.append_candidate = None
    st.session_state.append_local_path = ""
    st.session_state.appending = False
    request_rerun()

def append_to_server(ftp_client, local_path, remote_filename):
    """Agrega el contenido de un archivo local al final de un archivo remoto usando APPE."""
    try:
        # Forzando binario para evitar conflictos de tipo
        force_binary_type(ftp_client)
        
        # Usar cmd_STOR_APPE_STOU con el comando APPE
        success, message = client.cmd_STOR_APPE_STOU(ftp_client, local_path, remote_filename, command="APPE")
        return success, message
        
    except Exception as e:
        return False, e

