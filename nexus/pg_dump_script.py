#!/usr/bin/env python3
import os
import subprocess
from datetime import datetime


def dump_database():
    # Variables de entorno
    host = os.getenv("POSTGRES_HOST", "localhost")
    db = os.getenv("POSTGRES_DB", "nexus")
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd = os.getenv("POSTGRES_PASSWORD", "password")
    port = os.getenv("POSTGRES_PORT", "5432")

    # Nombre del archivo dump con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dump_file = f"{db}_backup_{timestamp}.sql"

    # Comando pg_dump
    cmd = [
        "pg_dump",
        f"--host={host}",
        f"--port={port}",
        f"--username={user}",
        f"--dbname={db}",
        "--verbose",
        "--clean",
        "--no-owner",
        "--no-privileges",
        f"--file={dump_file}"
    ]
    print(' '.join(cmd))
    # Ejecutar dump
    env = os.environ.copy()
    env["PGPASSWORD"] = pwd

    try:
        subprocess.run(cmd, env=env, check=True)
        print(f"✅ Dump creado: {dump_file}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    dump_database()