import os
import json
import keyring

class DbToolsError(Exception):
    pass

CONN_FILE = os.path.expanduser("~/.db_tools_connections.json")
SERVICE_NAME = "db-tools"

def load_connections():
    if not os.path.exists(CONN_FILE):
        return {}
    with open(CONN_FILE, "r") as f:
        conns = json.load(f)
    for name, conn in conns.items():
        password = keyring.get_password(SERVICE_NAME, f"{name}_password")
        if password:
            conn["password"] = password
    return conns

def save_connections(conns):
    for name, conn in conns.items():
        if "password" in conn:
            keyring.set_password(SERVICE_NAME, f"{name}_password", conn["password"])
            # Don't store the password in the JSON file
            del conn["password"]
    with open(CONN_FILE, "w") as f:
        json.dump(conns, f, indent=2)