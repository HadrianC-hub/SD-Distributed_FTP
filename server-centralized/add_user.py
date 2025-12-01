#!/usr/bin/env python3
import os
import json
import hashlib
import hmac
import getpass

USERS_FILE = os.path.join(os.path.dirname(__file__), "users.json")

# ----------- Funciones auxiliares (idénticas al servidor) -----------

def hash_password(password: str) -> str:
    """Genera un hash PBKDF2 seguro (compatible con server.py)."""
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 260000)
    return f"pbkdf2_sha256$260000${salt.hex()}${dk.hex()}"

def verify_password(stored_hash: str, password: str) -> bool:
    """Verifica contraseña (solo para validación opcional)."""
    try:
        algo, iter_str, salt_hex, hash_hex = stored_hash.split("$")
        salt = bytes.fromhex(salt_hex)
        new_hash = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, int(iter_str))
        return hmac.compare_digest(new_hash.hex(), hash_hex)
    except Exception:
        return False

def load_users():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            return json.load(f)
    return {}

def save_users(users):
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=2)

# ----------- Interfaz CLI -----------

def main():
    print("=== Crear nuevo usuario FTP ===")

    username = input("Nombre de usuario: ").strip()
    if not username:
        print("❌ Usuario no puede estar vacío.")
        return

    users = load_users()
    if username in users:
        print(f"⚠️ El usuario '{username}' ya existe.")
        choice = input("¿Deseas sobrescribirlo? (s/n): ").lower()
        if choice != "s":
            print("Operación cancelada.")
            return

    password = getpass.getpass("Contraseña: ")
    confirm = getpass.getpass("Confirmar contraseña: ")
    if password != confirm:
        print("❌ Las contraseñas no coinciden.")
        return

    users[username] = {"password": hash_password(password)}
    save_users(users)
    print(f"✅ Usuario '{username}' agregado correctamente a {USERS_FILE}")

if __name__ == "__main__":
    main()
