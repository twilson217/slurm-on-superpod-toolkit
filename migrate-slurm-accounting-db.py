#!/usr/bin/env python3
"""
Migrate Slurm accounting database from a Slurm controller to the BCM head node.

This script is intended to be run on the ACTIVE BCM head node as root.
It will:
  - Discover current Slurm accounting DB connection details from slurmdbd.conf
  - Dump the existing Slurm accounting database from the current StorageHost
  - Import that dump into the local MariaDB/MySQL instance on the BCM head

IMPORTANT:
  - This script ONLY migrates the database contents.
  - It does NOT change Slurm/BCM configuration (StorageHost, overlays, etc.).
    After migration, you must update the configuration (via cmsh / overlays)
    to point Slurm's slurmdbd to the new DB location on the head node.
  - High availability (HA) for the DB on the head nodes is provided by the
    existing BCM HA MySQL replication configured by cmha-setup. The cmha
    `dbreclone` command is a repair tool for the CMDaemon database and is
    not used here for normal Slurm DB migration.
"""

import os
import sys
import subprocess
import shlex
from datetime import datetime
from pathlib import Path


def run_cmd(cmd, check=True, capture_output=False, shell=False):
    """Run a local command."""
    if capture_output:
        result = subprocess.run(
            cmd,
            shell=shell,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    else:
        result = subprocess.run(cmd, shell=shell)

    if check and result.returncode != 0:
        if capture_output:
            raise RuntimeError(
                f"Command failed: {cmd}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        else:
            raise RuntimeError(f"Command failed: {cmd}")

    return result


def ensure_root():
    """Ensure the script is run as root."""
    if os.geteuid() != 0:
        print("ERROR: This script must be run as root.", file=sys.stderr)
        sys.exit(1)


def find_slurmdbd_conf() -> str:
    """Locate slurmdbd.conf using common BCM/Slurm paths."""
    candidates = [
        "/cm/shared/apps/slurm/var/etc/slurmdbd.conf",
        "/cm/shared/apps/slurm/current/etc/slurmdbd.conf",
        "/cm/local/apps/slurm/var/etc/slurmdbd.conf",
        "/cm/local/apps/slurm/current/etc/slurmdbd.conf",
        "/etc/slurm/slurmdbd.conf",
        "/usr/local/etc/slurmdbd.conf",
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return ""


def parse_slurmdbd_conf(conf_path: str):
    """Parse slurmdbd.conf for StorageHost/User/Pass/Loc/Port."""
    cfg = {
        "storage_host": None,
        "storage_port": "3306",
        "storage_user": None,
        "storage_pass": None,
        "storage_loc": "slurm_acct_db",
    }

    with open(conf_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip().lower()
            value = value.strip()
            if key == "storagehost":
                cfg["storage_host"] = value
            elif key == "storageport":
                cfg["storage_port"] = value
            elif key == "storageuser":
                cfg["storage_user"] = value
            elif key == "storagepass":
                cfg["storage_pass"] = value
            elif key == "storageloc":
                cfg["storage_loc"] = value

    missing = [k for k, v in cfg.items() if v is None]
    if missing:
        raise RuntimeError(
            f"Missing required keys in slurmdbd.conf ({conf_path}): {', '.join(missing)}"
        )
    return cfg


def detect_mysql_socket() -> str:
    """Try to detect a usable local MySQL/MariaDB socket path."""
    candidates = [
        "/var/lib/mysql/mysql.sock",
        "/var/run/mysqld/mysqld.sock",
        "/tmp/mysql.sock",
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    # Fallback: let mysql decide (may still work with TCP if configured)
    return ""


def dump_remote_slurm_db(cfg, dump_path: Path):
    """Dump the remote Slurm accounting DB using mysqldump from this head node.
    
    Uses options for maximum MySQL/MariaDB compatibility:
    - --default-character-set=utf8mb4: Ensures consistent character encoding
    - --single-transaction: Consistent snapshot without locking
    - --routines: Include stored procedures
    - --triggers: Include triggers (usually default, but explicit is safer)
    - --events: Include scheduled events
    - No --databases flag: Avoids including CREATE DATABASE in dump (we create it explicitly)
    """
    storage_host = cfg["storage_host"]
    storage_user = cfg["storage_user"]
    storage_pass = cfg["storage_pass"]
    storage_loc = cfg["storage_loc"]

    print(f"Dumping Slurm accounting DB from {storage_host} ...")
    dump_dir = dump_path.parent
    dump_dir.mkdir(parents=True, exist_ok=True)

    # Build mysqldump command with MySQL/MariaDB compatibility options
    cmd = [
        "mysqldump",
        "-h", storage_host,
        "-u", storage_user,
        f"-p{storage_pass}",
        "--single-transaction",
        "--routines",
        "--triggers",
        "--events",
        "--default-character-set=utf8mb4",
        storage_loc,  # Database name without --databases flag
    ]

    with open(dump_path, "w") as out_f:
        result = subprocess.run(cmd, stdout=out_f, stderr=subprocess.PIPE, text=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"mysqldump failed (host={storage_host}, db={storage_loc}):\n{result.stderr}"
        )

    print(f"  Dump created at: {dump_path}")


def import_db_to_local(cfg, dump_path: Path):
    """Import the dumped DB into local MariaDB/MySQL on the BCM head node.
    
    Creates the database with utf8mb4 charset, imports the dump, and
    creates the Slurm user with mysql_native_password authentication
    for maximum compatibility between MySQL and MariaDB.
    """
    storage_loc = cfg["storage_loc"]
    storage_user = cfg["storage_user"]
    storage_pass = cfg["storage_pass"]

    socket_path = detect_mysql_socket()
    mysql_base = ["mysql"]
    if socket_path:
        mysql_base.extend(["--socket", socket_path])

    print("Creating database on local MariaDB/MySQL ...")
    create_db_sql = (
        f"CREATE DATABASE IF NOT EXISTS `{storage_loc}` "
        f"DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    )
    run_cmd(mysql_base + ["-e", create_db_sql])

    print("Importing dump into local database ... (this may take a while)")
    # Use --default-character-set for import as well
    import_cmd = mysql_base + ["--default-character-set=utf8mb4", storage_loc]
    with open(dump_path, "r") as in_f:
        result = subprocess.run(
            import_cmd,
            stdin=in_f,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    if result.returncode != 0:
        raise RuntimeError(
            f"mysql import failed into local DB {storage_loc}:\n{result.stderr}"
        )

    print("Granting privileges to Slurm DB user on local MariaDB/MySQL ...")
    # Use mysql_native_password for compatibility between MySQL 8.x and MariaDB
    # MariaDB syntax: IDENTIFIED VIA mysql_native_password USING PASSWORD('...')
    # MySQL syntax: IDENTIFIED WITH mysql_native_password BY '...'
    # We try MariaDB syntax first since BCM head nodes typically run MariaDB
    grant_sql_mariadb = (
        f"CREATE USER IF NOT EXISTS '{storage_user}'@'%' "
        f"IDENTIFIED VIA mysql_native_password USING PASSWORD('{storage_pass}'); "
        f"GRANT ALL PRIVILEGES ON `{storage_loc}`.* TO '{storage_user}'@'%'; "
        f"FLUSH PRIVILEGES;"
    )
    
    # Try MariaDB syntax first
    result = subprocess.run(
        mysql_base + ["-e", grant_sql_mariadb],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    
    if result.returncode != 0:
        # Fall back to MySQL 8.x / generic syntax
        print("  MariaDB syntax failed, trying MySQL syntax...")
        grant_sql_mysql = (
            f"CREATE USER IF NOT EXISTS '{storage_user}'@'%' "
            f"IDENTIFIED WITH mysql_native_password BY '{storage_pass}'; "
            f"GRANT ALL PRIVILEGES ON `{storage_loc}`.* TO '{storage_user}'@'%'; "
            f"FLUSH PRIVILEGES;"
        )
        result2 = subprocess.run(
            mysql_base + ["-e", grant_sql_mysql],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result2.returncode != 0:
            # Last resort: simple syntax (works on older versions)
            print("  MySQL syntax failed, trying simple syntax...")
            grant_sql_simple = (
                f"CREATE USER IF NOT EXISTS '{storage_user}'@'%' "
                f"IDENTIFIED BY '{storage_pass}'; "
                f"GRANT ALL PRIVILEGES ON `{storage_loc}`.* TO '{storage_user}'@'%'; "
                f"FLUSH PRIVILEGES;"
            )
            run_cmd(mysql_base + ["-e", grant_sql_simple])


def main():
    ensure_root()

    print("=== Slurm Accounting DB Migration to BCM Head Node ===")
    conf_path = find_slurmdbd_conf()
    if not conf_path:
        print(
            "ERROR: Could not find slurmdbd.conf in standard locations. "
            "Run this on a BCM head node with shared Slurm configuration.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Using slurmdbd.conf: {conf_path}")
    cfg = parse_slurmdbd_conf(conf_path)

    hostname = run_cmd(["hostname"], capture_output=True).stdout.strip()

    print("\nCurrent Slurm accounting DB configuration (from slurmdbd.conf):")
    print(f"  StorageHost : {cfg['storage_host']}")
    print(f"  StoragePort : {cfg['storage_port']}")
    print(f"  StorageLoc  : {cfg['storage_loc']}")
    print(f"  StorageUser : {cfg['storage_user']}")
    print(f"\nTarget BCM head node for DB migration: {hostname}")
    print(
        "\nThis script will:\n"
        "  1) Use mysqldump from this head node to dump the Slurm accounting DB\n"
        f"     from StorageHost={cfg['storage_host']}.\n"
        f"  2) Import that dump into the local MariaDB/MySQL instance on {hostname}.\n"
        "  3) Grant the same Slurm DB user access to the local DB.\n"
        "\nIt WILL NOT automatically change StorageHost or any BCM overlays.\n"
        "After migration, you must update the Slurm accounting configuration (via cmsh)\n"
        "to point StorageHost to this head node (or its HA virtual hostname).\n"
    )

    answer = input("Proceed with migration? [y/N]: ").strip().lower()
    if answer not in ("y", "yes"):
        print("Aborting at user request.")
        sys.exit(0)

    # Suggest that Slurm services be stopped or quiesced first
    print(
        "\nNOTE: For a consistent snapshot, you should stop or quiesce slurmdbd on the\n"
        "current StorageHost before running this migration, or at least ensure there\n"
        "are no schema changes occurring during the dump.\n"
    )

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    dump_dir = Path("/root/slurm-db-migration")
    dump_path = dump_dir / f"slurm_acct_db-{ts}.sql"

    try:
        dump_remote_slurm_db(cfg, dump_path)
        import_db_to_local(cfg, dump_path)
    except Exception as e:
        print(f"\nERROR during migration: {e}", file=sys.stderr)
        print(f"Dump file (if created) is at: {dump_path}", file=sys.stderr)
        sys.exit(1)

    print("\n=== Migration completed successfully ===")
    print(f"  Dump file preserved at: {dump_path}")
    print(
        "\nNext steps (manual):\n"
        "  1) Use cmsh to update the Slurm accounting configuration overlay so that:\n"
        "       - StorageHost points to this BCM head node (or HA virtual hostname)\n"
        "       - Primary accounting server / AccountingStorageHost reflect the new DB host\n"
        "  2) Ensure MySQL HA between head nodes is healthy:\n"
        "       - Run: cmha status\n"
        "       - Verify 'mysql' status is OK for both head nodes\n"
        "  3) Restart slurmdbd on the accounting nodes so they connect to the new DB.\n"
    )


if __name__ == "__main__":
    main()


