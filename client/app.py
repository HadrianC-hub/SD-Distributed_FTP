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

# --- FUNCIONES PARA STOU ---

def start_stou_upload():
    """Inicia el proceso de subida con STOU."""
    st.session_state.stou_upload_candidate = True
    st.session_state.stou_upload_path = ""
    st.session_state.stou_multiple_files = []

def confirm_and_stou_upload():
    """Confirma y ejecuta la subida con STOU."""
    if not st.session_state.stou_multiple_files:
        st.error("❌ No se han seleccionado archivos")
        return
    
    uploaded_count = 0
    error_count = 0
    error_messages = []
    
    st.session_state.stou_uploading = True
    
    for file_path in st.session_state.stou_multiple_files:
        if os.path.exists(file_path) and os.path.isfile(file_path):
            success, message = stou_upload_to_server(st.session_state.ftp_client, file_path)
            if success:
                uploaded_count += 1
                log_message(f"✅ Subida STOU exitosa: {file_path}")
            else:
                error_count += 1
                error_messages.append(f"{os.path.basename(file_path)}: {message}")
                log_message(f"❌ Error en subida STOU: {file_path} - {message}")
        else:
            error_count += 1
            error_messages.append(f"{file_path}: No es un archivo válido")
    
    # Mostrar resumen
    if uploaded_count > 0:
        st.success(f"✅ {uploaded_count} archivo(s) subido(s) exitosamente con STOU")
    
    if error_count > 0:
        st.error(f"❌ {error_count} archivo(s) con errores")
        for error_msg in error_messages:
            st.error(f"   - {error_msg}")
    
    # Limpiar el estado de subida
    st.session_state.stou_upload_candidate = None
    st.session_state.stou_upload_path = ""
    st.session_state.stou_multiple_files = []
    st.session_state.stou_uploading = False
    request_rerun()

def cancel_stou_upload():
    """Cancela el proceso de subida STOU."""
    st.session_state.stou_upload_candidate = None
    st.session_state.stou_upload_path = ""
    st.session_state.stou_multiple_files = []
    st.session_state.stou_uploading = False
    request_rerun()

def stou_upload_to_server(ftp_client, local_path):
    """Sube un archivo individual al servidor usando STOU."""
    try:
        force_binary_type(ftp_client)
        
        # Usar STOU para subida única
        success, message = client.cmd_STOR_APPE_STOU(ftp_client, local_path, command="STOU")
        return success, message
        
    except Exception as e:
        return False, f"Error al subir {local_path} con STOU: {e}"

def add_file_to_stou_list():
    """Agrega un archivo a la lista de subida STOU."""
    file_path = st.session_state.stou_upload_path.strip()
    
    if not file_path:
        st.error("❌ La ruta no puede estar vacía")
        return
    
    if not os.path.exists(file_path):
        st.error("❌ La ruta especificada no existe")
        return
    
    if os.path.isdir(file_path):
        st.error("❌ STOU solo admite archivos individuales, no carpetas")
        return
    
    if file_path in st.session_state.stou_multiple_files:
        st.warning("⚠️ El archivo ya está en la lista")
    else:
        st.session_state.stou_multiple_files.append(file_path)
        st.success(f"✅ Archivo agregado: {os.path.basename(file_path)}")
    
    # Limpiar el campo de entrada
    st.session_state.stou_upload_path = ""
    request_rerun()

def remove_file_from_stou_list(index):
    """Elimina un archivo de la lista de subida STOU."""
    if 0 <= index < len(st.session_state.stou_multiple_files):
        removed_file = st.session_state.stou_multiple_files.pop(index)
        log_message(f"🗑️ Archivo removido de la lista: {removed_file}")
        request_rerun()

# --- FUNCIONES PARA SUBIDA ---

def start_upload():
    """Inicia el proceso de subida."""
    st.session_state.upload_candidate = True
    st.session_state.upload_path = ""

def confirm_and_upload():
    """Confirma y ejecuta la subida."""
    upload_path = st.session_state.upload_path.strip()
    
    if not upload_path:
        st.error("❌ La ruta no puede estar vacía")
        return
    
    if not os.path.exists(upload_path):
        st.error("❌ La ruta especificada no existe")
        return
    
    success, message = upload_to_server(st.session_state.ftp_client, upload_path)
    
    if success:
        st.success(f"✅ {message}")
        log_message(f"📤 Subida exitosa: {upload_path}")
    else:
        st.error(f"❌ {message}")
        log_message(f"💥 Error en subida: {upload_path} - {message}")
    
    # Limpiar el estado de subida
    st.session_state.upload_candidate = None
    st.session_state.upload_path = ""
    st.session_state.uploading = False
    request_rerun()

def cancel_upload():
    """Cancela el proceso de subida."""
    st.session_state.upload_candidate = None
    st.session_state.upload_path = ""
    st.session_state.uploading = False
    request_rerun()

def upload_to_server(ftp_client, local_path):
    """Sube un archivo o carpeta al servidor FTP."""
    try:
        force_binary_type(ftp_client)
        
        # Usar la función recursiva para manejar tanto archivos como carpetas
        success, message = store_recursive(ftp_client, local_path)
        return success, message
        
    except Exception as e:
        return False, f"Error al subir {local_path}: {e}"
    
def store_recursive(ftp_socket, local_path, remote_base_path=""):
    """Sube recursivamente una carpeta local al servidor FTP."""
    try:
        # Normalizar la ruta local
        local_path = os.path.abspath(local_path)
        
        if not os.path.exists(local_path):
            return False, f"La ruta local no existe: {local_path}"
        
        uploaded_files = 0
        uploaded_dirs = 0
        errors = []
        
        if os.path.isfile(local_path):
            # Es un archivo individual
            file_name = os.path.basename(local_path)
            remote_path = os.path.join(remote_base_path, file_name).replace("\\", "/")
            
            success, message = client.cmd_STOR_APPE_STOU(ftp_socket, local_path, remote_path, command="STOR")
            if success:
                uploaded_files += 1
                return True, f"Archivo subido: {file_name}"
            else:
                return False, f"Error subiendo archivo {file_name}: {message}"
                
        elif os.path.isdir(local_path):
            # Es una carpeta - subir recursivamente
            dir_name = os.path.basename(local_path)
            
            # Crear el directorio remoto
            remote_dir_path = os.path.join(remote_base_path, dir_name).replace("\\", "/")
            try:
                mkdir_response = client.generic_command_by_type(ftp_socket, remote_dir_path, command="MKD", command_type='A')
                if not mkdir_response.startswith('2'):
                    # El directorio podría ya existir, continuar
                    log_message(f"⚠️ No se pudo crear directorio {remote_dir_path}: {mkdir_response}")
            except Exception as e:
                log_message(f"⚠️ Error creando directorio {remote_dir_path}: {e}")
            
            # Recorrer el contenido del directorio local
            for item in os.listdir(local_path):
                local_item_path = os.path.join(local_path, item)
                
                if os.path.isfile(local_item_path):
                    # Subir archivo
                    remote_file_path = os.path.join(remote_dir_path, item).replace("\\", "/")
                    success, message = client.cmd_STOR_APPE_STOU(ftp_socket, local_item_path, remote_file_path, command="STOR")
                    if success:
                        uploaded_files += 1
                        log_message(f"✅ Subido: {item}")
                    else:
                        error_msg = f"Error con {item}: {message}"
                        errors.append(error_msg)
                        log_message(f"❌ {error_msg}")
                        
                elif os.path.isdir(local_item_path):
                    # Llamada recursiva para subdirectorios
                    success, message = store_recursive(ftp_socket, local_item_path, remote_dir_path)
                    if success:
                        uploaded_dirs += 1
                        log_message(f"✅ Directorio subido: {item}")
                    else:
                        error_msg = f"Error con directorio {item}: {message}"
                        errors.append(error_msg)
                        log_message(f"❌ {error_msg}")

            if errors:
                return False, f"Subida parcial. Archivos: {uploaded_files}, Carpetas: {uploaded_dirs}. Errores: {len(errors)}"
            else:
                return True, f"Subida completada. Archivos: {uploaded_files}, Carpetas: {uploaded_dirs}"
                
    except Exception as e:
        return False, f"Error en subida recursiva: {e}"

# --- RENOMBRADO DE ARCHIVOS Y CARPETAS ---

def start_renaming(item_name, item_type):
    """Inicia el proceso de renombrado."""
    st.session_state.renaming_candidate = (item_name, item_type)
    st.session_state.new_name = item_name  # Inicializar con el nombre actual

def confirm_rename():
    """Confirma y ejecuta el renombrado."""
    if not st.session_state.renaming_candidate:
        return
    
    old_name, item_type = st.session_state.renaming_candidate
    new_name = st.session_state.new_name.strip()
    
    if not new_name:
        st.error("❌ El nuevo nombre no puede estar vacío")
        return
    if old_name == new_name:
        st.error("❌ El nuevo nombre debe ser diferente al actual")
        return
    
    # Intentar RNFR
    success = True
    error = ""
    response = client.generic_command_by_type(st.session_state.ftp_client, old_name, command="RNFR", command_type='A')
    if response:
        log_message(response)

    if not response.startswith('3'):
        error = response
        success = False
    
    # Intentar RNTO
    if success:
        response = client.generic_command_by_type(st.session_state.ftp_client, new_name, command="RNTO", command_type='A')

        if not response.startswith('2'):
            success = False
            error = response
    
    if success:
        st.success(response)
        log_message(f"✏️ Renombrado {item_type}: {old_name} -> {new_name}")
    else:
        log_message(f"❌ Error renombrando {item_type}: {old_name} -> {new_name} - {message}")
        st.error(error)
    
    # Limpiar el estado de renombrado
    st.session_state.renaming_candidate = None
    st.session_state.new_name = ""
    request_rerun()

def cancel_rename():
    """Cancela el proceso de renombrado."""
    st.session_state.renaming_candidate = None
    st.session_state.new_name = ""
    request_rerun()

# --- CREACIÓN DE DIRECTORIO ---

def start_folder_creation():
    """Inicia el proceso de creación de carpeta."""
    st.session_state.creating_folder = True
    st.session_state.new_folder_name = ""

def confirm_create_folder():
    """Confirma y crea la nueva carpeta."""
    folder_name = st.session_state.new_folder_name.strip()
    
    if not folder_name:
        st.error("❌ El nombre de la carpeta no puede estar vacío")
        return
    
    success, message = create_folder(st.session_state.ftp_client, folder_name)
    
    if success:
        st.success(message)
        log_message(f"📁 Carpeta creada: {folder_name} - {message}")
    else:
        log_message(f"❌ Error creando carpeta: {folder_name} - {message}")
        st.error(message)
    
    # Limpiar el estado de creación
    st.session_state.creating_folder = False
    st.session_state.new_folder_name = ""
    request_rerun()

def create_folder(ftp_client, folder_name):
    """Crea un directorio usando el comando MKD."""
    try:
        response = client.generic_command_by_type(ftp_client, folder_name, command="MKD", command_type='A')
        return True, response
    except Exception as e:
        return False, f"Error al crear directorio: {e}"

def cancel_create_folder():
    """Cancela la creación de carpeta."""
    st.session_state.creating_folder = False
    st.session_state.new_folder_name = ""
    request_rerun()

