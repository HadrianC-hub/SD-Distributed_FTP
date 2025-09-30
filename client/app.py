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

