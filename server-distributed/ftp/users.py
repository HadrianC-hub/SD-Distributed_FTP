import os
from scripts.add_user import load_users, verify_password

ROOT = os.path.dirname(os.path.abspath(__file__))
USERS_FILE = os.path.normpath(os.path.join(ROOT, "../users/users.json"))
USERS = load_users()