import os
import random
import string

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))   # carpeta donde está server.py
SERVER_ROOT = os.path.join(BASE_DIR, "data")                                # raíz real del servidor

def generate_unique_filename(directory, original_filename):
    name, ext = os.path.splitext(original_filename)
    while True:
        unique_suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        unique_name = f"{name}_{unique_suffix}{ext}"
        if not os.path.exists(os.path.join(directory, unique_name)):
            return unique_name

def safe_path(session, path):
    """
    Devuelve la ruta normalizada dentro del root del session.
    Lanza PermissionError si la ruta sale del root.
    """
    # Si el path es absoluto (comienza con /), lo interpretamos como relativo al root del usuario
    if path.startswith('/'):
        # Remover el slash inicial y tratarlo como ruta relativa al root_dir
        relative_path = path[1:]
        candidate = os.path.normpath(os.path.join(session.root_dir, relative_path))
    else:
        candidate = os.path.normpath(os.path.join(session.current_dir, path))
    
    # Obtener rutas absolutas
    candidate_abs = os.path.abspath(candidate)
    root_abs = os.path.abspath(session.root_dir)
    
    # Verificar que la ruta esté dentro del root del usuario
    if not candidate_abs.startswith(root_abs):
        raise PermissionError("Access outside of user root")
    
    return candidate_abs

def is_valid_filename(name):
    """
    Valida si un nombre de archivo/carpeta es válido para el sistema operativo.
    Retorna (es_valido, mensaje_error)
    """
    # Nombres reservados en Windows
    reserved_names = {
        'CON', 'PRN', 'AUX', 'NUL',
        'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
        'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
    }
    # Caracteres prohibidos en la mayoría de sistemas operativos
    invalid_chars = '<>:"/\\|?*'

    if not name or not name.strip():
        return False, "Filename cannot be empty"
    
    # Verificar caracteres inválidos
    for char in invalid_chars:
        if char in name:
            return False, f"Character '{char}' is not allowed"
    
    name_upper = name.upper()
    # Remover extensión para la verificación
    base_name = name_upper.split('.')[0]
    
    if base_name in reserved_names:
        return False, f"'{name}' is a reserved system name"
    
    # Verificar nombres que terminan con punto o espacio
    if name.endswith('.') or name.endswith(' '):
        return False, "Filename cannot end with dot or space"
    
    # Longitud máxima típica
    if len(name) > 255:
        return False, "Filename too long (max 255 characters)"
    
    # Verificar caracteres de control (ASCII < 32)
    for char in name:
        if ord(char) < 32:
            return False, "Control characters are not allowed"
    
    return True, "Valid"

def calculate_file_hash(filepath):
    """
    Calcula hash MD5 de un archivo.
    Función centralizada para evitar duplicación.
    """
    import hashlib
    hash_md5 = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        print(f"[HASH] Error calculando hash de {filepath}: {e}")
        return ""
