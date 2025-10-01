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

# --- BORRADO DE ARCHIVOS Y CARPETAS ---

def confirm_and_delete(item_name, item_type):
    """Maneja la confirmación y eliminación de archivos/directorios."""
    if item_type == "file":
        success, message = delete_file(st.session_state.ftp_client, item_name)
    else:
        # Para directorios, usar eliminación recursiva
        success, message = delete_directory_recursive(st.session_state.ftp_client, item_name)
    
    if success:
        st.success(message)
        log_message(f"🗑️ Eliminado {item_type}: {item_name} - {message}")
    else:
        st.error(message)
        log_message(f"❌ Error eliminando {item_type}: {item_name} - {message}")
    
    # Limpiar el estado de confirmación
    st.session_state.delete_candidate = None
    request_rerun()

def delete_file(ftp_client, filename):
    """Elimina un archivo usando el comando DELE."""
    try:
        response = client.generic_command_by_type(ftp_client, filename, command="DELE", command_type='A')
        return True, response
    except Exception as e:
        return False, f"Error al eliminar archivo: {e}"

def delete_directory(ftp_client, dirname):
    """Elimina un directorio usando el comando RMD."""
    try:
        response = client.generic_command_by_type(ftp_client, dirname, command="RMD", command_type='A')
        return True, response
    except Exception as e:
        return False, f"Error al eliminar directorio: {e}"

def delete_directory_recursive(ftp_socket, dir_name):
    """Elimina recursivamente una carpeta y todo su contenido."""
    try:
        # Guardar directorio actual ANTES de cualquier cambio
        original_dir = get_current_dir(ftp_socket)
        log_message(f"🔍 Iniciando eliminación recursiva de: {dir_name} desde {original_dir}")
        
        # Cambiar al directorio a eliminar
        success, message = change_dir(ftp_socket, dir_name)
        if not success:
            return False, f"No se pudo acceder al directorio: {dir_name} - {message}"
        
        current_remote_dir = get_current_dir(ftp_socket)
        log_message(f"📁 Cambiado a directorio: {current_remote_dir}")
        
        # Obtener listado del directorio
        items, messages = list_directory(ftp_socket)
        for msg in messages:
            log_message(f"ℹ️ {msg}")
        
        deleted_files = 0
        deleted_dirs = 0
        errors = []
        
        # Si no hay items (directorio vacío), proceder a eliminar el directorio
        if not items:
            log_message(f"📁 Directorio vacío: {dir_name}")
        else:
            # Procesar archivos primero
            for item in items:
                if item["name"] in [".", ".."]:
                    continue
                    
                if item["type"] == "file":
                    log_message(f"🗑️ Eliminando archivo: {item['name']}")
                    success, message = delete_file(ftp_socket, item["name"])
                    if success:
                        deleted_files += 1
                        log_message(f"✅ Eliminado: {item['name']}")
                    else:
                        error_msg = f"Error con {item['name']}: {message}"
                        errors.append(error_msg)
                        log_message(f"❌ {error_msg}")
                        
            # Procesar subdirectorios después
            for item in items:
                if item["name"] in [".", ".."]:
                    continue
                    
                if item["type"] == "dir":
                    log_message(f"📁 Procesando subdirectorio: {item['name']}")
                    
                    success, message = delete_directory_recursive(ftp_socket, item["name"])
                    
                    if success:
                        deleted_dirs += 1
                        log_message(f"✅ Subdirectorio eliminado: {item['name']}")
                    else:
                        error_msg = f"Error con directorio {item['name']}: {message}"
                        errors.append(error_msg)
                        log_message(f"❌ {error_msg}")

        # Volver al directorio original para eliminar la carpeta principal
        log_message(f"↩️ Volviendo al directorio original para eliminar {dir_name}")
        change_dir(ftp_socket, "..")

        # Ahora eliminar el directorio principal (que debería estar vacío)
        log_message(f"🗑️ Eliminando directorio principal: {dir_name}")
        success, message = delete_directory(ftp_socket, dir_name)
        
        if not success:
            error_msg = f"Error eliminando directorio principal {dir_name}: {message}"
            errors.append(error_msg)
            log_message(f"❌ {error_msg}")
        else:
            log_message(f"✅ Directorio principal eliminado: {dir_name}")

        if errors:
            return False, f"Eliminación parcial. Archivos: {deleted_files}, Carpetas: {deleted_dirs}. Errores: {len(errors)}"
        else:
            return True, f"Eliminación completada. Archivos: {deleted_files}, Carpetas: {deleted_dirs}"
            
    except Exception as e:
        log_message(f"💥 Error crítico en eliminación recursiva: {e}")
        # Intentar volver al directorio original incluso en caso de error
        try:
            change_dir(ftp_socket, original_dir)
        except:
            pass
        return False, f"Error en eliminación recursiva: {e}"
    
# --- ALTERNAR MODO Y TIPO DE TRANSFERENCIA ---

def toggle_transfer_mode():
    """Alterna entre los modos Stream y Block"""
    try:
        # Usar el estado de la sesión como fuente de verdad
        current_mode = st.session_state.transfer_mode
        new_mode = 'B' if current_mode == 'S' else 'S'
        
        # Enviar comando MODE al servidor
        response = client.generic_command_by_type(st.session_state.ftp_client, new_mode, command="MODE", command_type='A')
        
        if response.startswith('2'):
            # Actualizar ambos: estado de sesión y variable global
            st.session_state.transfer_mode = new_mode
            client.MODE = new_mode
            mode_name = "Block" if new_mode == 'B' else "Stream"
            log_message(f"✅ Modo de transferencia cambiado a: {mode_name}")
            return True, f"Modo cambiado a {mode_name}"
        else:
            log_message(f"❌ Error al cambiar modo: {response}")
            return False, f"Error del servidor: {response}"
            
    except Exception as e:
        error_msg = f"Error al cambiar modo de transferencia: {e}"
        log_message(f"💥 {error_msg}")
        return False, error_msg

def toggle_transfer_type():
    """Alterna entre los tipos ASCII y Binario"""
    try:
        # Usar el estado de la sesión como fuente de verdad
        current_type = st.session_state.transfer_type
        new_type = 'I' if current_type == 'A' else 'A'
        
        # Enviar comando TYPE al servidor
        response = client.generic_command_by_type(st.session_state.ftp_client, new_type, command="TYPE", command_type='A')
        
        if response.startswith('2'):
            # Actualizar ambos: estado de sesión y variable global
            st.session_state.transfer_type = new_type
            client.TYPE = new_type
            type_name = "Binario" if new_type == 'I' else "ASCII"
            log_message(f"✅ Tipo de transferencia cambiado a: {type_name}")
            return True, f"Tipo cambiado a {type_name}"
        else:
            log_message(f"❌ Error al cambiar tipo: {response}")
            return False, f"Error del servidor: {response}"
            
    except Exception as e:
        error_msg = f"Error al cambiar tipo de transferencia: {e}"
        log_message(f"💥 {error_msg}")
        return False, error_msg

def get_transfer_mode_display():
    """Obtiene el texto para mostrar el modo actual"""
    current_mode = st.session_state.transfer_mode
    return "Block" if current_mode == 'B' else "Stream"

def get_transfer_type_display():
    """Obtiene el texto para mostrar el tipo actual"""
    current_type = st.session_state.transfer_type
    return "Binario" if current_type == 'I' else "ASCII"

# --- REALIZAR DESCARGA ---

def confirm_and_download(item_name, item_type):
    """Maneja la confirmación y descarga de archivos/directorios."""

    ensure_download_dir()
    
    if item_type == "file":
        # Para archivos, descargar directamente en la carpeta de descargas
        local_path = os.path.join(st.session_state.download_path, item_name)
        success, message = download_file(st.session_state.ftp_client, item_name, local_path)
    else:
        # Para carpetas, usar descarga recursiva
        success, message = download_directory_recursive(st.session_state.ftp_client, item_name, st.session_state.download_path)
    
    if success:
        st.success(message)
        log_message(f"🎉 Descarga exitosa de {item_type}: {item_name}")
        log_message(f"↩️ Volviendo a directorio original")
        change_dir(st.session_state.ftp_client, "..")

    else:
        st.error(message)
        log_message(f"💥 Error descargando {item_type}: {item_name} - {message}")
    
    # Limpiar el estado de confirmación
    st.session_state.download_candidate = None
    request_rerun()

def ensure_download_dir():
    """Asegura que el directorio de descargas exista."""
    download_path = os.path.abspath(st.session_state.download_path)
    if not os.path.exists(download_path):
        os.makedirs(download_path, exist_ok=True)
        log_message(f"📁 Directorio de descargas creado: {download_path}")
    st.session_state.download_path = download_path  # Actualizar a ruta absoluta

def download_file(ftp_client, remote_filename, local_path):
    """Descarga un archivo individual usando RETR."""
    try:
        # Asegurar que el directorio local existe
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Usar ruta absoluta
        local_path = os.path.abspath(local_path)
        
        log_message(f"📄 Iniciando descarga: {remote_filename} -> {local_path}")
        
        # FORZAR TIPO BINARIO para archivos
        try:
            binary_response = client.generic_command_by_type(ftp_client, "I", command="TYPE", command_type='A')
            log_message(f"🔧 Cambiando a tipo binario: {binary_response}")

            # 🔥 Sincronizar estado
            st.session_state.transfer_type = 'I'
            client.TYPE = 'I'

        except Exception as e:
            log_message(f"⚠️ No se pudo cambiar a binario: {e}")
        
        # Descargar archivo
        result = client.cmd_RETR(ftp_client, remote_filename, local_path)
        
        # Verificar si cmd_RETR retornó una tupla (success, message)
        if isinstance(result, tuple) and len(result) == 2:
            success, message = result
        else:
            # Si no retornó una tupla, asumir error
            success = False
            message = f"Respuesta inesperada de cmd_RETR: {result}"
        
        if success:
            log_message(f"✅ Archivo descargado exitosamente: {local_path}")
            return True, f"Archivo descargado: {os.path.basename(local_path)}"
        else:
            log_message(f"❌ Error en descarga: {message}")
            return False, message
            
    except Exception as e:
        error_msg = f"Error al descargar archivo {remote_filename}: {e}"
        log_message(f"💥 {error_msg}")
        return False, error_msg

def download_directory_recursive(ftp_client, remote_dir, local_base_path):
    """Descarga recursivamente una carpeta y todo su contenido."""
    try: 
        # Guardar directorio actual ANTES de cualquier cambio
        original_dir = get_current_dir(ftp_client)
        log_message(f"🔍 Iniciando descarga de: {remote_dir} desde {original_dir}")
        
        # FORZAR TIPO BINARIO al inicio de la descarga recursiva
        try:
            binary_response = client.generic_command_by_type(ftp_client, "I", command="TYPE", command_type='A')
            log_message(f"🔧 Cambiando a tipo binario: {binary_response}")

            # 🔥 Sincronizar estado
            st.session_state.transfer_type = 'I'
            client.TYPE = 'I'

        except Exception as e:
            log_message(f"⚠️ No se pudo cambiar a binario: {e}")
        
        # Cambiar al directorio remoto
        success, message = change_dir(ftp_client, remote_dir)
        if not success:
            return False, f"No se pudo acceder al directorio: {remote_dir} - {message}"
        
        current_remote_dir = get_current_dir(ftp_client)
        log_message(f"📁 Cambiado a directorio remoto: {current_remote_dir}")
        
        # Crear directorio local - usar solo el nombre de la carpeta, no la ruta completa
        local_dir = os.path.join(local_base_path, os.path.basename(remote_dir))
        os.makedirs(local_dir, exist_ok=True)
        log_message(f"📂 Directorio local creado: {local_dir}")
        
        # Obtener listado del directorio
        items, messages = list_directory(ftp_client)
        for msg in messages:
            log_message(f"ℹ️ {msg}")
        
        downloaded_files = 0
        downloaded_dirs = 0
        errors = []
        
        # Si no hay items (directorio vacío), igualmente considerarlo éxito
        if not items:
            log_message(f"📁 Directorio vacío: {remote_dir}")
        
        # Procesar archivos primero
        for item in items:
            if item["name"] in [".", ".."]:
                continue
                
            if item["type"] == "file":
                local_file_path = os.path.join(local_dir, item["name"])
                log_message(f"⬇️ Descargando archivo: {item['name']} -> {local_file_path}")
                
                success, message = download_file(ftp_client, item["name"], local_file_path)
                if success:
                    downloaded_files += 1
                    log_message(f"✅ Descargado: {item['name']}")
                else:
                    error_msg = f"Error con {item['name']}: {message}"
                    errors.append(error_msg)
                    log_message(f"❌ {error_msg}")
                    
        # Procesar subdirectorios después
        for item in items:
            if item["name"] in [".", ".."]:
                continue
                
            if item["type"] == "dir":
                log_message(f"📁 Procesando subdirectorio: {item['name']}")
                
                # Guardar el directorio actual antes de la recursión
                current_before_recursion = get_current_dir(ftp_client)
                
                success, message = download_directory_recursive(ftp_client, item["name"], local_dir)
                
                if success:
                    downloaded_dirs += 1
                    log_message(f"✅ Subdirectorio descargado: {item['name']}")
                else:
                    error_msg = f"Error con directorio {item['name']}: {message}"
                    errors.append(error_msg)
                    log_message(f"❌ {error_msg}")
                
                # Volver al directorio anterior usando ".." en lugar de rutas absolutas
                log_message(f"↩️ Volviendo al directorio padre usando '..' desde {get_current_dir(ftp_client)}")
                success, _ = change_dir(ftp_client, "..")
                if not success:
                    # Si falla con "..", intentar volver al directorio guardado
                    log_message(f"⚠️ No se pudo volver al directorio padre")

        if errors:
            return False, f"Descarga parcial. Archivos: {downloaded_files}, Carpetas: {downloaded_dirs}. Errores: {len(errors)}"
        else:
            return True, f"Descarga completada. Archivos: {downloaded_files}, Carpetas: {downloaded_dirs}"
            
    except Exception as e:
        log_message(f"💥 Error crítico en descarga recursiva: {e}")
        # Intentar volver al directorio original incluso en caso de error
        try:
            change_dir(ftp_client, original_dir)
        except:
            pass
        return False, f"Error en descarga recursiva: {e}"

# --- FUNCIONES PARA DESCARGA CON PORT ---

def start_download_with_port(item_name, item_type):
    """Inicia el proceso de descarga usando el comando PORT."""
    st.session_state.download_port_candidate = (item_name, item_type)
    st.session_state.using_port_mode = True
    # Inicializar valores por defecto para PORT
    if "port_ip" not in st.session_state:
        st.session_state.port_ip = "127.0.0.1"
    if "port_port" not in st.session_state:
        st.session_state.port_port = "22"

def confirm_and_download_with_port():
    """Maneja la confirmación y descarga usando el comando PORT."""
    if not st.session_state.download_port_candidate:
        return

    item_name, item_type = st.session_state.download_port_candidate

    # Solo permitir archivos individuales para PORT (no carpetas)
    if item_type != "file":
        st.error("❌ El modo PORT solo está disponible para archivos individuales")
        st.session_state.download_port_candidate = None
        st.session_state.using_port_mode = False
        request_rerun()
        return

    ensure_download_dir()
    
    # Obtener IP y Puerto del usuario
    ip = st.session_state.port_ip
    port = st.session_state.port_port
    
    if not ip or not port:
        st.error("❌ Debe especificar IP y Puerto para usar el modo PORT")
        return
    
    # Configurar PORT y luego descargar
    local_path = os.path.join(st.session_state.download_path, item_name)
    success, message = download_file_with_port(st.session_state.ftp_client, item_name, local_path, ip, port)
    
    if success:
        st.success(message)
        log_message(f"🎉 Descarga con PORT exitosa: {item_name}")
    else:
        st.error(message)
        log_message(f"💥 Error en descarga con PORT: {item_name} - {message}")
    
    # Limpiar el estado
    st.session_state.download_port_candidate = None
    st.session_state.using_port_mode = False
    request_rerun()

def download_file_with_port(ftp_client, remote_filename, local_path, ip, port):
    """Descarga un archivo individual usando el comando PORT."""
    try: 
        # Asegurar que el directorio local existe
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        local_path = os.path.abspath(local_path)
        
        log_message(f"📄 Iniciando descarga con PORT: {remote_filename} -> {local_path}")
        log_message(f"🔌 Configurando PORT con IP: {ip}, Puerto: {port}")
        
        force_binary_type(ftp_client)
        
        # Configurar el comando PORT
        port_success, port_message = setup_port_command(ftp_client, ip, port)
        if not port_success:
            return False, f"Error configurando PORT: {port_message}"
        
        # Descargar archivo usando RETR (que ahora usará el socket configurado por PORT)
        result = client.cmd_RETR(ftp_client, remote_filename, local_path)
        
        # Verificar si cmd_RETR retornó una tupla (success, message)
        if isinstance(result, tuple) and len(result) == 2:
            success, message = result
        else:
            success = False
            message = f"Respuesta inesperada de cmd_RETR: {result}"
        
        if success:
            log_message(f"✅ Archivo descargado exitosamente con PORT: {local_path}")
            return True, f"Archivo descargado con PORT: {os.path.basename(local_path)}"
        else:
            log_message(f"❌ Error en descarga con PORT: {message}")
            return False, message
            
    except Exception as e:
        error_msg = f"Error al descargar archivo {remote_filename} con PORT: {e}"
        log_message(f"💥 {error_msg}")
        return False, error_msg

def setup_port_command(ftp_client, ip, port):
    """Configura el comando PORT para transferencia activa."""
    try:
        # Enviar comando PORT con la IP y puerto proporcionados
        response = client.cmd_PORT(ftp_client, ip, port)
        
        if response and response.startswith('2'):
            log_message(f"✅ Comando PORT exitoso - IP: {ip}, Puerto: {port}")
            log_message(response)
            return True, "PORT configurado correctamente"
        else:
            log_message(response)
            return False, f"Error en comando PORT: {response}"
            
    except Exception as e:
        return False, f"Error configurando PORT: {e}"

# --- GESTIONES DE DIRECTORIOS ---

def get_current_dir(ftp_socket):
    """Obtiene el directorio actual."""
    response = client.generic_command_by_type(ftp_socket, command="PWD", command_type='B')
    if isinstance(response, str) and response.startswith('2'):
        match = re.search(r'"(.+)"', response)
        return match.group(1) if match else "/"
    return "/"

def list_directory(ftp_socket, path=None):
    """
    Lista archivos y carpetas en el servidor usando LIST.
    Devuelve información detallada incluyendo permisos, propietario, tamaño, etc.
    """

    old_stdout = sys.stdout
    sys.stdout = mystdout = io.StringIO()

    try:
        if path:
            result = client.cmd_LIST_NLST(ftp_socket, path, command="LIST")
        else:
            result = client.cmd_LIST_NLST(ftp_socket, command="LIST")
        
        if result:
            log_message(result)
    except Exception as e:
        log_message(f"Error: {e}")
    finally:
        sys.stdout = old_stdout

    raw_output = mystdout.getvalue()
    lines = raw_output.splitlines()
    
    items = []
    messages = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Filtrar respuestas del servidor (códigos numéricos)
        if re.match(r'^\d{3}', line):
            messages.append(line)
            continue

        # Filtrar líneas de información como "total X"
        if line.lower().startswith('total '):
            messages.append(line)
            continue

        # Parsear formato UNIX estándar (ls -l)
        # Ejemplo: drwxr-xr-x 2 user group 4096 Dec 31 23:59 directory
        if len(line) > 10 and line[0] in 'd-l' and any(c in line[1:10] for c in 'rwx-'):
            # Intentar dividir la línea de manera más robusta
            parts = line.split(None, 8)  # Dividir en máximo 9 partes
            if len(parts) >= 9:
                try:
                    permissions = parts[0]
                    links = parts[1]
                    owner = parts[2]
                    group = parts[3]
                    size = parts[4]
                    month = parts[5]
                    day = parts[6]
                    time_or_year = parts[7]
                    name = parts[8]
                    
                    # Validar que sea un elemento válido
                    if name in ['.', '..']:
                        continue
                    
                    # Determinar tipo
                    if permissions.startswith('d'):
                        item_type = "dir"
                    elif permissions.startswith('l'):
                        item_type = "link"
                    else:
                        item_type = "file"
                    
                    # Formatear fecha
                    date_str = f"{month} {day} {time_or_year}"
                    
                    items.append({
                        "name": name,
                        "type": item_type,
                        "permissions": permissions,
                        "links": links,
                        "owner": owner,
                        "group": group,
                        "size": size,
                        "date": date_str,
                        "raw_line": line
                    })
                    continue
                except Exception as e:
                    messages.append(f"Error parsing line: {line} - {e}")
                    continue

        # Si llegamos aquí, la línea no coincide con el formato esperado
        # Intentar extraer al menos el nombre y tipo básico
        if len(line) > 2 and not line.isspace():
            # Para líneas que podrían tener nombres con espacios, buscar desde el final
            # Asumir que los primeros campos son permisos, links, owner, group, size, date
            # y el resto es el nombre
            parts = line.split()
            if len(parts) >= 8:
                # Intentar determinar el tipo por el primer carácter
                if line[0] == 'd':
                    item_type = "dir"
                elif line[0] == 'l':
                    item_type = "link"
                else:
                    item_type = "file"
                
                # El nombre es todo lo que viene después de los primeros 7 campos
                name_parts = parts[7:]
                name = ' '.join(name_parts)
                
                if name not in ['.', '..'] and not re.match(r'^\d{3}', name):
                    items.append({
                        "name": name,
                        "type": item_type,
                        "permissions": parts[0] if len(parts) > 0 else "",
                        "links": parts[1] if len(parts) > 1 else "",
                        "owner": parts[2] if len(parts) > 2 else "",
                        "group": parts[3] if len(parts) > 3 else "",
                        "size": parts[4] if len(parts) > 4 else "",
                        "date": f"{parts[5]} {parts[6]}" if len(parts) > 6 else "",
                        "raw_line": line
                    })
                    messages.append(f"Advanced parse: {line}")

    return items, messages

def change_dir(ftp_socket, target):
    """Cambia de directorio."""
    try:
        # Manejo especial para el directorio padre ".."
        if target == "..":
            response = client.generic_command_by_type(ftp_socket, command="CDUP", command_type="B")
        else:
            # Si el objetivo tiene espacios, asegurarse de que se maneje correctamente
            if ' ' in target:
                # Intentar primero sin comillas (comportamiento normal)
                response = client.generic_command_by_type(ftp_socket, target, command="CWD", command_type="A")
                if not (isinstance(response, str) and response.startswith('2')):
                    # Si falla, intentar con comillas
                    quoted_target = f'"{target}"'
                    response = client.generic_command_by_type(ftp_socket, quoted_target, command="CWD", command_type="A")
            else:
                response = client.generic_command_by_type(ftp_socket, target, command="CWD", command_type="A")
        
        if isinstance(response, str) and response.startswith('2'):
            new_dir = get_current_dir(ftp_socket)
            return True, new_dir
        else:
            return False, response
    except Exception as e:
        return False, f"Error: {e}"

# --- FUNCIONES DE CONSOLA ---

def log_message(msg: str):
    st.session_state.console.append(str(msg))

def toggle_console():
    st.session_state.show_console = not st.session_state.show_console

def clear_console():
    st.session_state.console = []

# --- GESTIÓN DE CONEXIONES Y NAVEGACIÓN ---

def generate_connection_id():
    """Genera un ID único para la conexión basado en credenciales y timestamp"""
    connection_string = f"{st.session_state.host}:{st.session_state.port}:{st.session_state.usuario}:{time.time()}"
    # Usar hash() nativo de Python y convertir a hexadecimal
    return hex(hash(connection_string))[-8:]

def start_connection():
    """Callback del botón conectar."""
    
    # Generar nuevo ID de conexión
    st.session_state.connection_id = generate_connection_id()
    
    # Guardar las credenciales en el estado de la sesión
    st.session_state.host = st.session_state.host
    st.session_state.port = st.session_state.port
    st.session_state.usuario = st.session_state.usuario
    st.session_state.password = st.session_state.password
    
    success, ftp_client, message = client.connect_to_ftp(st.session_state.host,st.session_state.port,st.session_state.usuario,st.session_state.password)

    log_message(message if message else "Conexión establecida")

    if success and "Login successful" in (message if message else ""):
        st.session_state.ftp_client = ftp_client
        st.session_state.current_dir = "/"
        st.session_state.keep_alive_started = False
        
        # Sincronizar client con el estado de la sesión
        client.MODE = st.session_state.transfer_mode
        client.TYPE = st.session_state.transfer_type
        
        # Forzar rerun inmediato
        request_rerun()

    else:
        st.session_state.ftp_client = None

def restart_connection():
    """Reinicia la conexión FTP usando el comando REIN y luego reconecta automáticamente"""
    # Guardar las credenciales actuales antes de reiniciar
    host = st.session_state.host
    port = st.session_state.port
    usuario = st.session_state.usuario
    password = st.session_state.password
    if st.session_state.ftp_client:
        try:
            # Enviar comando REIN para reiniciar la conexión
            response = client.generic_command_by_type(st.session_state.ftp_client, command="REIN", command_type='B')
            log_message(response)
            
            # Cerrar el socket de manera más agresiva
            st.session_state.ftp_client.close()

            try:
                client.cleanup_data_socket()
            except Exception:
                pass


        except Exception as e:
            log_message(f"Error en REIN: {e}")
        finally:
            st.session_state.ftp_client = None
    
    # Pequeña pausa para asegurar que el socket se cerró
    time.sleep(2)
    
    # Generar nuevo ID de conexión
    new_connection_id = generate_connection_id()
    st.session_state.connection_id = new_connection_id
    
    log_message("🔄 Intentando reconexión automática...")

    success, ftp_client, message = client.connect_to_ftp(host, port, usuario, password)

    if message:
        log_message(message)
    
    if success and "Login successful" in (message if message else ""):
        st.session_state.ftp_client = ftp_client
        st.session_state.keep_alive_started = False

        # Sincronizar configuraciones después de reconectar
        client.MODE = st.session_state.transfer_mode
        client.TYPE = st.session_state.transfer_type

        log_message("✅ Reconexión exitosa")
    else:
        log_message("❌ Reconexión fallida - Volviendo a pantalla de login")
        st.session_state.ftp_client = None
        try:
            client.cleanup_data_socket()
        except Exception:
            pass

    request_rerun()

def handle_directory_navigation(target):
    """Maneja la navegación a directorios."""
    if st.session_state.navigation_lock:
        st.warning("🔄 Navegación en progreso...")
        return
    
    st.session_state.navigation_lock = True
    
    try:
        ftp = st.session_state.ftp_client
        success, new_dir_or_msg = change_dir(ftp, target)
        
        if success:
            st.session_state.current_dir = new_dir_or_msg
            st.success(f"✅ Directorio cambiado a: {new_dir_or_msg}")
        else:
            st.error(f"❌ No se pudo abrir '{target}': {new_dir_or_msg}")
    except Exception as e:
        st.error(f"❌ Error al cambiar directorio: {e}")
    finally:
        st.session_state.navigation_lock = False
        request_rerun()

# --- OTRAS FUNCIONES AUXILIARES ---

def force_binary_type(ftp_client):
        try:
            binary_response = client.generic_command_by_type(ftp_client, "I", command="TYPE", command_type='A')
            log_message(f"🔧 Cambiando a tipo binario: {binary_response}")
            st.session_state.transfer_type = 'I'
            client.TYPE = 'I'
        except Exception as e:
            log_message(f"⚠️ No se pudo cambiar a binario: {e}")

# -----------------------------------------------------------------------------------------------------
# Barra lateral
# -----------------------------------------------------------------------------------------------------
st.sidebar.title("📊 Panel de Control")

# Estado de conexión
if st.session_state.ftp_client:
    st.sidebar.success(f"🔗 Conectado - ID: {st.session_state.connection_id}")
else:
    st.sidebar.warning("🔌 No conectado")

st.sidebar.markdown("---")

# --- GRUPO 1: CONEXIÓN ---
if st.session_state.ftp_client:
    st.sidebar.subheader("🔗 Conexión")
    
    col_conn1, col_conn2 = st.sidebar.columns(2)
    
    with col_conn1:
        if st.button("🔄 Reiniciar", use_container_width=True, key="restart_btn", help="Reiniciar la conexión FTP"):
            restart_connection()
    
    with col_conn2:
        if st.button("🚪 Desconectar", use_container_width=True, key="disconnect_btn", type="secondary", help="Cerrar sesión FTP"):
            if st.session_state.ftp_client:
                try:
                    client.generic_command_by_type(st.session_state.ftp_client, command="QUIT", command_type='B')
                except:
                    pass
                try:
                    st.session_state.ftp_client.close()
                except:
                    pass
            st.session_state.ftp_client = None
            st.session_state.keep_alive_started = False
            request_rerun()

    st.sidebar.markdown("---")

# --- GRUPO 2: GESTIÓN DE ARCHIVOS ---
if st.session_state.ftp_client:
    st.sidebar.subheader("📁 Gestión de Archivos")
    
    # Botones de gestión de archivos
    if st.sidebar.button("📁 Crear Carpeta", 
                use_container_width=True,
                key="create_folder_sidebar"):
        start_folder_creation()
    
    if st.sidebar.button("📤 Subir Archivos", 
                use_container_width=True,
                key="upload_sidebar"):
        start_upload()
    
    if st.sidebar.button("🦄 Subir STOU", 
                use_container_width=True,
                key="stou_sidebar",
                help="Subir con nombre único"):
        start_stou_upload()

    st.sidebar.markdown("---")

# --- GRUPO 3: CONFIGURACIÓN DE TRANSFERENCIA ---
if st.session_state.ftp_client:
    st.sidebar.subheader("⚡ Transferencia")
    
    # Configuración de modo y tipo
    col_trans1, col_trans2 = st.sidebar.columns(2)
    
    with col_trans1:
        current_mode = get_transfer_mode_display()
        mode_icon = "🔄" if st.session_state.transfer_mode == 'S' else "📦"
        if st.button(f"{mode_icon} {current_mode}", 
                    use_container_width=True,
                    key="mode_toggle",
                    help=f"Cambiar a {'Block' if st.session_state.transfer_mode == 'S' else 'Stream'}"):
            success, message = toggle_transfer_mode()
            if success:
                st.sidebar.success(message)
            else:
                st.sidebar.error(message)
            request_rerun()
    
    with col_trans2:
        current_type = get_transfer_type_display()
        type_icon = "📝" if st.session_state.transfer_type == 'A' else "🔢"
        if st.button(f"{type_icon} {current_type}", 
                    use_container_width=True,
                    key="type_toggle",
                    help=f"Cambiar a {'Binario' if st.session_state.transfer_type == 'A' else 'ASCII'}"):
            success, message = toggle_transfer_type()
            if success:
                st.sidebar.success(message)
            else:
                st.sidebar.error(message)
            request_rerun()
    
    # Mostrar estado actual
    st.sidebar.caption(f"Modo: {current_mode} | Tipo: {current_type}")

    st.sidebar.markdown("---")

# --- GRUPO 4: CONFIGURACIÓN DE DESCARGAS ---
if st.session_state.ftp_client:
    with st.sidebar.expander("📥 Configuración Descargas", expanded=False):
        download_path = st.text_input(
            "Directorio de descargas:",
            value=st.session_state.download_path,
            key="download_path_input",
            help="Ruta donde se guardarán las descargas"
        )
        st.session_state.download_path = download_path
        
        if st.button("📁 Crear Directorio", 
                    use_container_width=True,
                    key="create_download_dir"):
            ensure_download_dir()
            st.sidebar.success(f"✅ Directorio listo")
        
        # Mostrar ruta absoluta actual
        st.caption(f"Ruta: {os.path.abspath(st.session_state.download_path)}")

    st.sidebar.markdown("---")

# --- GRUPO 5: CONSOLA Y MANTENIMIENTO ---
st.sidebar.subheader("🔧 Utilidades")

# Botón de consola
console_icon = "🖥️" if not st.session_state.show_console else "📺"
console_text = "Mostrar Consola" if not st.session_state.show_console else "Ocultar Consola"
if st.sidebar.button(f"{console_icon} {console_text}", 
            use_container_width=True,
            key="console_toggle_btn"):
    toggle_console()

# --- ESPACIADO FINAL ---
st.sidebar.markdown("")
st.sidebar.markdown("")
st.sidebar.markdown("")
st.sidebar.caption("Cliente FTP - v1.0")
st.sidebar.caption("Desarrollado por Adrian Hernández Castellanos y Laura Martir Beltrán")
    
# -----------------------------------------------------------------------------------------------------
# Página de gestión FTP
# -----------------------------------------------------------------------------------------------------
if st.session_state.ftp_client:

    st.title("Gestión FTP")
    
    if st.sidebar.button("Desconectar"):
        if st.session_state.ftp_client:
            try:
                client.generic_command_by_type(st.session_state.ftp_client, command="QUIT", command_type='B')
            except:
                pass
            try:
                st.session_state.ftp_client.close()
            except:
                pass
        # limpieza adicional para sockets de datos persistentes
        try:
            client.cleanup_data_socket()
        except Exception:
            pass

        st.session_state.ftp_client = None
        request_rerun()

    # Obtener directorio actual
    try:
        current_dir = get_current_dir(st.session_state.ftp_client)
        st.session_state.current_dir = current_dir
    except Exception as e:
        st.error(f"No se pudo obtener el directorio actual: {e}")
        current_dir = st.session_state.current_dir

    # Barra de direcciones
    st.text_input("Ruta actual:", value=current_dir, disabled=True)

    # Listado de archivos y carpetas
    try:
        items, messages = list_directory(st.session_state.ftp_client)
        for msg in messages:
            log_message(msg)
    except Exception as e:
        st.error(f"No se pudo listar el directorio: {e}")
        items = []

    # Después de obtener items, messages
    st.write("Contenido del directorio:")

    # Diálogo de creación de carpeta
    if st.session_state.creating_folder:
        st.markdown("---")
        st.subheader("Crear Nueva Carpeta")
        
        folder_name = st.text_input(
            "Nombre de la carpeta:",
            value=st.session_state.new_folder_name,
            key="new_folder_input",
            placeholder="Ingresa el nombre de la carpeta..."
        )
        st.session_state.new_folder_name = folder_name
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Crear", use_container_width=True):
                confirm_create_folder()
        with col2:
            if st.button("❌ Cancelar", use_container_width=True, key="cancel_folder"):
                cancel_create_folder()

    # Preparar los elementos para la tabla - SIEMPRE mostrar tabla si no estamos en raíz
    # Filtrar elementos válidos
    valid_items = []
    if items:
        valid_items = [item for item in items if item.get("name") and item["name"] not in [".", ".."]]
    
    # Agregar el elemento ".." al principio si no estamos en la raíz
    # Esto asegura que siempre podamos navegar hacia atrás
    if current_dir != "/":
        parent_item = {
            "name": "..",
            "type": "dir",
            "permissions": "",
            "links": "",
            "owner": "",
            "group": "",
            "size": "",
            "date": "",
            "raw_line": "Parent directory"
        }
        valid_items.insert(0, parent_item)
    
    # Ordenar elementos: primero carpetas, luego archivos, alfabéticamente
    if valid_items:
        # Separar carpetas y archivos
        folders = [item for item in valid_items if item["type"].lower() == "dir"]
        files = [item for item in valid_items if item["type"].lower() == "file"]
        
        # Ordenar alfabéticamente
        folders_sorted = sorted(folders, key=lambda x: x["name"].lower())
        files_sorted = sorted(files, key=lambda x: x["name"].lower())
        
        # Reconstruir la lista: primero ".." (si existe), luego carpetas, luego archivos
        valid_items = []
        
        # Agregar ".." primero si existe
        parent_item = None
        for item in folders:
            if item["name"] == "..":
                parent_item = item
                break
        
        if parent_item:
            valid_items.append(parent_item)
            # Remover ".." de folders_sorted para no duplicarlo
            folders_sorted = [item for item in folders_sorted if item["name"] != ".."]
        
        # Agregar el resto de carpetas ordenadas
        valid_items.extend(folders_sorted)
        # Agregar archivos ordenados
        valid_items.extend(files_sorted)

    # Mostrar tabla si hay elementos o si no estamos en la raíz (para mostrar "..")
    if valid_items or current_dir != "/":

        # Encabezados de la tabla con columna de acciones
        col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([0.4, 1, 0.8, 0.8, 0.8, 0.8, 0.8, 1.8])
        
        with col1:
            st.write("**Tipo**")
        with col2:
            st.write("**Nombre**")
        with col3:
            st.write("**Permisos**")
        with col4:
            st.write("**Propietario**")
        with col5:
            st.write("**Grupo**")
        with col6:
            st.write("**Tamaño**")
        with col7:
            st.write("**Fecha**")
        with col8:
            st.write("**Acciones**")
        
        st.markdown("---")
        
        # Mostrar cada elemento en una fila de la tabla
        for item in valid_items:
            col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([0.4, 1, 0.8, 0.8, 0.8, 0.8, 0.8, 1.8])
            
            with col1:
                if item["type"].lower() == "dir":
                    st.write("📁")
                elif item["type"].lower() == "link":
                    st.write("🔗")
                else:
                    st.write("📄")
            
            with col2:
                if item["type"].lower() == "dir":
                    button_key = f"folder_{current_dir}_{item['name']}"
                    if st.button(item["name"], key=button_key, 
                            help=f"Clic para entrar al directorio {item['name']}"):
                        handle_directory_navigation(item["name"])
                else:
                    st.write(item["name"])
            
            with col3:
                # Mostrar permisos o guión si está vacío
                permissions = item.get("permissions", "")
                st.write(permissions if permissions else "-")
            
            with col4:
                owner = item.get("owner", "")
                st.write(owner if owner else "-")
            
            with col5:
                group = item.get("group", "")
                st.write(group if group else "-")
            
            with col6:
                size = item.get("size", "")
                st.write(size if size else "-")
            
            with col7:
                date = item.get("date", "")
                st.write(date if date else "-")

            with col8:
                # Crear un contenedor para los botones de acción
                action_container = st.container()
                
                with action_container:
                    col_del, col_down, col_rename, col_port, col_append = st.columns(5)

                    with col_del:
                        # No mostrar botón de eliminar para el directorio padre ".."
                        if item["name"] != "..":
                            if st.button("🗑️", key=f"delete_{item['name']}", help=f"Eliminar {item['name']}"):
                                st.session_state.delete_candidate = (item['name'], item['type'])
                                request_rerun()

                    with col_down:
                        # Botón de descarga normal (PASV) para todos los elementos excepto ".."
                        if item["name"] != "..":
                            if st.button("⬇️", key=f"download_{item['name']}", help=f"Descargar {item['name']} (PASV)"):
                                st.session_state.download_candidate = (item['name'], item['type'])
                                request_rerun()

                    with col_rename:
                        # Botón de renombrar para todos los elementos excepto ".."
                        if item["name"] != "..":
                            if st.button("✏️", key=f"rename_{item['name']}", help=f"Renombrar {item['name']}"):
                                start_renaming(item['name'], item['type'])
                                request_rerun()

                    with col_port:
                        # Botón de descarga con PORT solo para archivos (no carpetas)
                        if item["name"] != ".." and item["type"] == "file":
                            if st.button("🔌", key=f"download_port_{item['name']}", help=f"Descargar {item['name']} con PORT"):
                                start_download_with_port(item['name'], item['type'])
                                request_rerun()

                    with col_append:
                        # Botón de append solo para archivos (no carpetas) y no para ".."
                        if item["name"] != ".." and item["type"] == "file":
                            if st.button("📎", key=f"append_{item['name']}", help=f"Append a {item['name']}"):
                                start_append(item['name'], item['type'])
                                request_rerun()
            
            # Línea separadora sutil entre elementos
            st.markdown("<hr style='margin: 2px 0; border: 0.5px solid #333;'>", 
                    unsafe_allow_html=True)
        
        # Diálogo de subida
        if st.session_state.upload_candidate:
            st.markdown("---")
            st.subheader("📤 Subir al Servidor")
            
            upload_path = st.text_input(
                "Ruta local del archivo o carpeta a subir:",
                value=st.session_state.upload_path,
                key="upload_path_input",
                placeholder="Ingresa la ruta absoluta del archivo o carpeta..."
            )
            st.session_state.upload_path = upload_path
            
            if upload_path:
                if os.path.exists(upload_path):
                    if os.path.isfile(upload_path):
                        st.info(f"📄 Archivo a subir: {os.path.basename(upload_path)}")
                    else:
                        st.info(f"📁 Carpeta a subir: {os.path.basename(upload_path)}")
                        st.warning("⚠️ Se subirá toda la carpeta y su contenido recursivamente")
                else:
                    st.error("❌ La ruta no existe")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Subir", use_container_width=True):
                    st.session_state.uploading = True
                    confirm_and_upload()
            with col2:
                if st.button("❌ Cancelar", use_container_width=True, key="cancel_upload"):
                    cancel_upload()

        # --- Diálogo de subida STOU ---
        if st.session_state.stou_upload_candidate:
            st.markdown("---")
            st.subheader("🦄 Subir Archivos con STOU")
            st.info("STOU: El servidor generará nombres únicos para cada archivo basados en sus nombres originales")
            
            # Mostrar lista actual de archivos
            st.write("**Archivos en la lista de subida:**")
            if st.session_state.stou_multiple_files:
                for i, file_path in enumerate(st.session_state.stou_multiple_files):
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.write(f"📄 {os.path.basename(file_path)}")
                        st.caption(f"Ruta: {file_path}")
                    with col2:
                        if st.button("🗑️", key=f"remove_stou_{i}", help=f"Eliminar {os.path.basename(file_path)} de la lista"):
                            remove_file_from_stou_list(i)
            else:
                st.info("💡 No hay archivos en la lista. Agrega archivos usando el campo de abajo.")
            
            st.markdown("---")
            st.write("**Agregar archivo a la lista:**")
            
            # Campo para agregar nueva ruta
            new_file_path = st.text_input(
                "Ruta del archivo a agregar:",
                value=st.session_state.stou_upload_path,
                key="stou_path_input",
                placeholder="Ingresa la ruta absoluta del archivo..."
            )
            st.session_state.stou_upload_path = new_file_path
            
            # Botones de acción en columnas
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("➕ Agregar Archivo", use_container_width=True, key="add_file_stou"):
                    add_file_to_stou_list()
            
            with col2:
                if st.session_state.stou_multiple_files:
                    if st.button("🚀 Subir Todos", use_container_width=True, key="confirm_stou", type="primary"):
                        confirm_and_stou_upload()
                else:
                    st.button("🚀 Subir Todos", use_container_width=True, disabled=True, key="confirm_stou_disabled")
            
            with col3:
                if st.button("❌ Cancelar", use_container_width=True, key="cancel_stou"):
                    cancel_stou_upload()
            
            # Información adicional
            if st.session_state.stou_multiple_files:
                st.info(f"📊 Total de archivos en lista: {len(st.session_state.stou_multiple_files)}")
            
        # Diálogo de append
        if st.session_state.append_candidate is not None:
            remote_name, item_type = st.session_state.append_candidate
            
            st.info(f"Append al archivo remoto: '{remote_name}'")
            st.warning("⚠️ El contenido del archivo local se agregará al final del archivo remoto.")
            
            local_path = st.text_input(
                "Ruta local del archivo a appendear:",
                value=st.session_state.append_local_path,
                key="append_local_path_input",
                placeholder="Ingresa la ruta absoluta del archivo local..."
            )
            st.session_state.append_local_path = local_path
            
            if local_path:
                if os.path.exists(local_path) and os.path.isfile(local_path):
                    st.info(f"📄 Archivo local: {os.path.basename(local_path)}")
                else:
                    st.error("❌ La ruta no existe o no es un archivo")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Append", use_container_width=True):
                    st.session_state.appending = True
                    confirm_and_append()
            with col2:
                if st.button("❌ Cancelar", use_container_width=True, key="cancel_append"):
                    cancel_append()

        # Diálogo de confirmación para eliminar
        if st.session_state.delete_candidate is not None:
            item_name, item_type = st.session_state.delete_candidate
            item_type_str = "archivo" if item_type == "file" else "carpeta"
            
            # Mostrar advertencia especial para carpetas
            if item_type == "dir":
                st.warning(f"⚠️ **ADVERTENCIA**: Se eliminará la carpeta '{item_name}' y **TODO SU CONTENIDO** de forma permanente.")
                st.error("🚨 **Esta acción no se puede deshacer**")
                st.info("📋 **Proceso**: Se eliminarán primero todos los archivos, luego las subcarpetas, y finalmente la carpeta principal.")
            else:
                st.warning(f"¿Estás seguro de que deseas eliminar el {item_type_str} '{item_name}'?")
            
            col1, col2 = st.columns(2)
            with col1:
                button_text = "🗑️ Sí, eliminar TODO" if item_type == "dir" else "🗑️ Sí, eliminar"
                button_type = "primary" if item_type == "dir" else "secondary"
                if st.button(button_text, use_container_width=True, type=button_type):
                    confirm_and_delete(item_name, item_type)
            with col2:
                if st.button("❌ Cancelar", use_container_width=True, key="cancel_delete"):
                    st.session_state.delete_candidate = None
                    request_rerun()

        # Diálogo de confirmación para descargar
        if st.session_state.download_candidate is not None:
            item_name, item_type = st.session_state.download_candidate
            item_type_str = "archivo" if item_type == "file" else "carpeta"
            
            st.info(f"¿Descargar el {item_type_str} '{item_name}' a '{st.session_state.download_path}'?")
            
            if item_type == "dir":
                st.warning("⚠️ La descarga de carpetas puede tomar tiempo dependiendo del contenido.")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Sí, descargar", use_container_width=True):
                    confirm_and_download(item_name, item_type)
            with col2:
                if st.button("❌ Cancelar", use_container_width=True, key="cancel_download"):
                    st.session_state.download_candidate = None
                    request_rerun()

        # Diálogo de confirmación para descarga con PORT
        if st.session_state.download_port_candidate is not None:
            item_name, item_type = st.session_state.download_port_candidate
            
            st.info(f"¿Descargar el archivo '{item_name}' usando el comando PORT (modo activo)?")
            st.warning("⚠️ El modo PORT requiere que el servidor pueda conectarse a tu cliente. Esto puede no funcionar en todas las configuraciones de red.")
            
            # Campos para configurar PORT
            col1, col2 = st.columns(2)
            with col1:
                port_ip = st.text_input(
                    "IP para PORT:",
                    value=st.session_state.port_ip,
                    key="port_ip_input",
                    help="IP que el servidor usará para conectarse a ti"
                )
                st.session_state.port_ip = port_ip
            
            with col2:
                port_port = st.text_input(
                    "Puerto para PORT:",
                    value=st.session_state.port_port,
                    key="port_port_input",
                    help="Puerto que el servidor usará para conectarse"
                )
                st.session_state.port_port = port_port
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Sí, usar PORT", use_container_width=True):
                    confirm_and_download_with_port()
            with col2:
                if st.button("❌ Cancelar", use_container_width=True, key="cancel_port"):
                    st.session_state.download_port_candidate = None
                    st.session_state.using_port_mode = False
                    request_rerun()

        # Diálogo de renombrado
        if st.session_state.renaming_candidate is not None:
            old_name, item_type = st.session_state.renaming_candidate
            item_type_str = "archivo" if item_type == "file" else "carpeta"
            
            st.info(f"Renombrar {item_type_str}: '{old_name}'")
            
            new_name = st.text_input(
                "Nuevo nombre:",
                value=st.session_state.new_name,
                key="new_name_input",
                placeholder=f"Ingresa el nuevo nombre para {old_name}..."
            )
            st.session_state.new_name = new_name
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Confirmar", use_container_width=True):
                    confirm_rename()
            with col2:
                if st.button("❌ Cancelar", use_container_width=True, key="cancel_rename"):
                    cancel_rename()
    else:
        # Solo mostrar este mensaje si estamos en la raíz y no hay elementos
        if current_dir == "/":
            st.info("El directorio raíz está vacío")
        else:
            # En directorios no raíz, siempre deberíamos tener al menos ".."
            st.info("No se encontraron archivos o directorios en esta carpeta")
# -----------------------------------------------------------------------------------------------------
# Página de login
# -----------------------------------------------------------------------------------------------------
else:
    st.title("Cliente FTP Distribuido")

    # Crear un contenedor centrado para el formulario de login
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        # Contenedor con borde y sombra para el formulario
        st.markdown("""
            <style>
            .login-container {
                background-color: #0e1117;
                padding: 30px;
                border-radius: 10px;
                border: 1px solid #262730;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                margin: 20px 0;
            }
            .login-title {
                text-align: center;
                margin-bottom: 25px;
                color: #fafafa;
                font-size: 1.5em;
            }
            </style>
        """, unsafe_allow_html=True)
        
        # Título del formulario
        st.markdown('<div class="login-title">🔐 Iniciar Sesión FTP</div>', unsafe_allow_html=True)
        
        # Inicializar credenciales si no existen
        if "host" not in st.session_state:
            st.session_state.host = "127.0.0.1"
        if "port" not in st.session_state:
            st.session_state.port = 21
        if "usuario" not in st.session_state:
            st.session_state.usuario = ""
        if "password" not in st.session_state:
            st.session_state.password = ""

        # Campos del formulario dentro del contenedor
        host_input = st.text_input("Servidor", value=st.session_state.host, key="host_input")
        port_input = st.number_input("Puerto", min_value=1, max_value=65535, value=st.session_state.port, key="port_input")
        usuario_input = st.text_input("Usuario", value=st.session_state.usuario, key="usuario_input")
        password_input = st.text_input("Contraseña", type="password", value=st.session_state.password, key="password_input")
        
        # Actualizar el estado de la sesión con los valores actuales de los campos
        st.session_state.host = host_input
        st.session_state.port = port_input
        st.session_state.usuario = usuario_input
        st.session_state.password = password_input

        # Botón de conexión centrado
        col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
        with col_btn2:
            if st.button("🚀 Conectar", use_container_width=True):
                start_connection()
        
        st.markdown('</div>', unsafe_allow_html=True)
    # Espacio adicional debajo del formulario
    st.markdown("<br><br>", unsafe_allow_html=True)
# -----------------------------------------------------------------------------------------------------
# Consola
# -----------------------------------------------------------------------------------------------------
if st.session_state.show_console:
    st.subheader("Consola")
    
    # Botón para limpiar consola
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🧹 Limpiar Consola"):
            clear_console()
            request_rerun()
    
    if not st.session_state.console:
        st.markdown("(Sin mensajes aún)")
    else:
        html_lines = []
        for msg in st.session_state.console:
            for line in msg.splitlines():
                stripped = line.strip()
                if not stripped:
                    color = "white"
                elif stripped[0] in ["1", "2"]:
                    color = "green"
                elif stripped[0] == "3":
                    color = "blue"
                elif stripped[0] in ["4", "5"]:
                    color = "red"
                else:
                    color = "white"
                escaped_line = line.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&#x27;")
                html_lines.append(f'<span style="color:{color}">{escaped_line}</span><br>')

        console_html = "".join(html_lines)
        
        container_id = f"console_{int(time.time() * 1000)}"
        
        st.markdown(
            f'''
            <div id="{container_id}" style="
                background:#071127; 
                padding:10px; 
                font-family:monospace; 
                height:300px;
                overflow-y:auto;
                border: 1px solid #333;
                border-radius:4px;
                white-space: pre-wrap;
                word-wrap: break-word;
            ">
                {console_html}
            </div>
            ''',
            unsafe_allow_html=True
        )

# Al final del script: si alguna función pidió rerun, ejecutarlo UNA sola vez (Problemas con Docker)
if st.session_state.pop("_need_rerun", False):
    cooldown = st.session_state.pop("_requested_rerun_cooldown", 0.5)
    if can_do_action("global_rerun", cooldown=cooldown):
        st.rerun()