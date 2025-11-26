#!/usr/bin/env python3
"""
Migrate Slurm accounting database from Slurm controllers to BCM head nodes.

This script is intended to be run on the ACTIVE BCM head node as root.
It will:
  1. Discover current Slurm accounting DB connection details from slurmdbd.conf
  2. Dump the existing Slurm accounting database from the current StorageHost
  3. Import that dump into the local MariaDB/MySQL instance on the BCM head
  4. Automatically update BCM configuration via cmsh:
     - Find the configuration overlay with the slurmaccounting role
     - Update the role's primary to the active BCM head node
     - Update the role's storagehost to "master" (BCM HA virtual hostname)
     - Update the overlay to use "allheadnodes yes" instead of specific nodes

After running this script:
  - The Slurm accounting database will be on the BCM head nodes
  - BCM's MySQL HA (via cmha) will provide database redundancy
  - slurmdbd will run on the head nodes instead of dedicated controllers

High availability (HA) for the DB on the head nodes is provided by the
existing BCM HA MySQL replication configured by cmha-setup.
"""

import os
import sys
import subprocess
import re
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


def run_ssh(host: str, cmd: str, timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a command on a remote host via SSH."""
    ssh_cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=5",
        host,
        cmd,
    ]
    return subprocess.run(
        ssh_cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_db_connectivity(cfg) -> tuple:
    """Test if we can connect to the remote database from this host.
    
    Returns:
        (success: bool, error_type: str, error_message: str)
        error_type can be: 'none', 'host_denied', 'auth_failed', 'connection_failed', 'other'
    """
    storage_host = cfg["storage_host"]
    storage_user = cfg["storage_user"]
    storage_pass = cfg["storage_pass"]
    storage_loc = cfg["storage_loc"]
    
    # Try a simple connection test
    cmd = [
        "mysql",
        "-h", storage_host,
        "-u", storage_user,
        f"-p{storage_pass}",
        "-e", "SELECT 1;",
        storage_loc,
    ]
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    
    if result.returncode == 0:
        return (True, 'none', '')
    
    stderr = result.stderr.lower()
    
    # Check for explicit host not allowed error
    # e.g., "Host 'hostname' is not allowed to connect to this MySQL server"
    if "host" in stderr and "not allowed" in stderr:
        return (False, 'host_denied', result.stderr.strip())
    
    # Check for access denied with host in the message
    # e.g., "Access denied for user 'slurm'@'hostname' (using password: YES)"
    # This happens when the user exists for some hosts (e.g., localhost) but not for this host
    if "access denied" in stderr and "@'" in stderr:
        # Extract the host from the error message to see if it's different from localhost
        import re
        match = re.search(r"@'([^']+)'", result.stderr)
        if match:
            denied_host = match.group(1).lower()
            # If the denied host is not localhost, it's a host permission issue
            if denied_host not in ('localhost', '127.0.0.1', '::1'):
                return (False, 'host_denied', result.stderr.strip())
    
    # Check for authentication failure (user doesn't exist or wrong password for localhost)
    if "access denied" in stderr and "using password" in stderr:
        return (False, 'auth_failed', result.stderr.strip())
    
    # Check for connection failure
    if "can't connect" in stderr or "connection refused" in stderr:
        return (False, 'connection_failed', result.stderr.strip())
    
    return (False, 'other', result.stderr.strip())


def check_remote_mysql_client(host: str) -> tuple:
    """Check if mysql client is available on a remote host.
    
    Returns:
        (available: bool, mysql_path: str)
    """
    # Check common locations
    result = run_ssh(host, "which mysql 2>/dev/null || command -v mysql 2>/dev/null")
    if result.returncode == 0 and result.stdout.strip():
        return (True, result.stdout.strip())
    
    # Check if it exists but not in PATH
    for path in ["/usr/bin/mysql", "/usr/local/bin/mysql"]:
        result = run_ssh(host, f"test -x {path} && echo {path}")
        if result.returncode == 0 and result.stdout.strip():
            return (True, result.stdout.strip())
    
    return (False, "")


def get_local_hostname_for_db() -> str:
    """Get the hostname/IP that the database server would see for connections from this host."""
    result = run_cmd(["hostname", "-f"], capture_output=True, check=False)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    
    result = run_cmd(["hostname"], capture_output=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else "localhost"


def fix_remote_db_permissions(cfg, mysql_path: str = "/usr/bin/mysql") -> bool:
    """SSH to the remote database host and grant access from any host.
    
    This updates the MySQL user to allow connections from '%' (any host).
    
    Args:
        cfg: Database configuration dictionary
        mysql_path: Path to mysql client on remote host
        
    Returns:
        True if permissions were updated successfully
    """
    storage_host = cfg["storage_host"]
    storage_user = cfg["storage_user"]
    storage_pass = cfg["storage_pass"]
    storage_loc = cfg["storage_loc"]
    
    print(f"\n  Attempting to fix database permissions on {storage_host}...")
    
    # Common socket paths to try
    socket_paths = [
        "/var/lib/mysql/mysql.sock",
        "/var/run/mysqld/mysqld.sock",
        "/tmp/mysql.sock",
    ]
    
    # Find a working socket on the remote host
    working_socket = None
    for socket_path in socket_paths:
        result = run_ssh(storage_host, f"test -S {socket_path} && echo exists")
        if result.returncode == 0 and "exists" in result.stdout:
            working_socket = socket_path
            break
    
    if not working_socket:
        print(f"    ✗ Could not find MySQL socket on {storage_host}")
        return False
    
    # Build the SQL to grant access from any host
    # We use socket auth as root to update the user permissions
    grant_sql = (
        f"GRANT ALL PRIVILEGES ON `{storage_loc}`.* TO '{storage_user}'@'%' "
        f"IDENTIFIED BY '{storage_pass}'; FLUSH PRIVILEGES;"
    )
    
    # Escape single quotes for shell
    grant_sql_escaped = grant_sql.replace("'", "'\"'\"'")
    
    # Run as root via socket authentication
    remote_cmd = f"{mysql_path} --socket={working_socket} -e '{grant_sql_escaped}'"
    
    print(f"    Running: ssh {storage_host} \"{mysql_path} --socket=... -e 'GRANT ...'\"")
    
    result = run_ssh(storage_host, remote_cmd, timeout=60)
    
    if result.returncode == 0:
        print(f"    ✓ Granted '{storage_user}'@'%' access to {storage_loc}")
        return True
    else:
        # Try alternative: maybe user already exists with localhost, need to create for %
        print(f"    First attempt failed, trying alternative syntax...")
        
        # Try CREATE USER IF NOT EXISTS with GRANT
        alt_sql = (
            f"CREATE USER IF NOT EXISTS '{storage_user}'@'%' IDENTIFIED BY '{storage_pass}'; "
            f"GRANT ALL PRIVILEGES ON `{storage_loc}`.* TO '{storage_user}'@'%'; "
            f"FLUSH PRIVILEGES;"
        )
        alt_sql_escaped = alt_sql.replace("'", "'\"'\"'")
        remote_cmd = f"{mysql_path} --socket={working_socket} -e '{alt_sql_escaped}'"
        
        result = run_ssh(storage_host, remote_cmd, timeout=60)
        
        if result.returncode == 0:
            print(f"    ✓ Created '{storage_user}'@'%' with access to {storage_loc}")
            return True
        else:
            print(f"    ✗ Failed to update permissions: {result.stderr.strip()}")
            return False


def ensure_db_connectivity(cfg) -> bool:
    """Ensure we can connect to the remote database, fixing permissions if needed.
    
    Returns:
        True if connectivity is established (or was fixed)
        False if we cannot connect and cannot fix it
    """
    storage_host = cfg["storage_host"]
    storage_user = cfg["storage_user"]
    local_hostname = get_local_hostname_for_db()
    
    print(f"\nTesting database connectivity to {storage_host}...")
    
    success, error_type, error_msg = test_db_connectivity(cfg)
    
    if success:
        print(f"  ✓ Successfully connected to database on {storage_host}")
        return True
    
    print(f"  ✗ Connection failed: {error_msg}")
    
    if error_type == 'host_denied':
        print(f"\n  The database user '{storage_user}' is not allowed to connect from this host.")
        print(f"  This host appears as: {local_hostname}")
        print(f"\n  Checking if we can fix this via SSH to {storage_host}...")
        
        # Check if mysql client is available on remote host
        mysql_available, mysql_path = check_remote_mysql_client(storage_host)
        
        if not mysql_available:
            print(f"\n  ✗ MySQL client not found on {storage_host}")
            print(f"\n  To fix this, please install the mysql client package on {storage_host}:")
            print(f"    # For Ubuntu/Debian:")
            print(f"    ssh {storage_host} 'apt-get update && apt-get install -y mariadb-client'")
            print(f"    # For RHEL/Rocky:")
            print(f"    ssh {storage_host} 'dnf install -y mariadb'")
            print(f"\n  Then run this script again.")
            return False
        
        print(f"  ✓ MySQL client found at: {mysql_path}")
        
        # Ask user for confirmation before modifying remote DB
        answer = input(f"\n  Update database permissions on {storage_host} to allow connections from this host? [y/N]: ").strip().lower()
        if answer not in ('y', 'yes'):
            print("  Aborting. Please fix database permissions manually.")
            return False
        
        # Try to fix permissions
        if fix_remote_db_permissions(cfg, mysql_path):
            # Test connectivity again
            print(f"\n  Re-testing database connectivity...")
            success, _, error_msg = test_db_connectivity(cfg)
            if success:
                print(f"  ✓ Successfully connected to database after permission fix!")
                return True
            else:
                print(f"  ✗ Still cannot connect: {error_msg}")
                return False
        else:
            return False
    
    elif error_type == 'auth_failed':
        print(f"\n  ✗ Authentication failed. Check StorageUser/StoragePass in slurmdbd.conf")
        return False
    
    elif error_type == 'connection_failed':
        print(f"\n  ✗ Cannot connect to MySQL server on {storage_host}")
        print(f"    Verify the MySQL/MariaDB service is running and accessible.")
        return False
    
    else:
        print(f"\n  ✗ Unknown error connecting to database")
        return False


def run_cmsh(cmsh_commands: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run cmsh commands and return the result.
    
    Args:
        cmsh_commands: Multi-line string of cmsh commands to execute
        check: If True, raise on non-zero exit code
        
    Returns:
        CompletedProcess with stdout/stderr
    """
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    if not os.path.exists(cmsh_path):
        raise RuntimeError(f"cmsh not found at {cmsh_path}")
    
    result = subprocess.run(
        [cmsh_path],
        input=cmsh_commands,
        capture_output=True,
        text=True,
    )
    
    if check and result.returncode != 0:
        raise RuntimeError(
            f"cmsh command failed:\n{cmsh_commands}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    
    return result


def get_primary_bcm_headnode() -> str:
    """Get the primary (active) BCM head node hostname.
    
    Uses cmha status to determine which head node is currently active.
    Falls back to local hostname if cmha is not available.
    """
    # Try cmha status first
    result = subprocess.run(
        ["cmha", "status"],
        capture_output=True,
        text=True,
    )
    
    if result.returncode == 0:
        # Parse output for active node (marked with *)
        for line in result.stdout.split('\n'):
            if '*' in line and '->' in line:
                # Format: "basecm11* -> head2" - the one with * is active
                match = re.search(r'(\S+)\*\s*->', line)
                if match:
                    return match.group(1)
    
    # Fallback to local hostname
    result = run_cmd(["hostname", "-s"], capture_output=True)
    return result.stdout.strip()


def find_slurmaccounting_overlay() -> str:
    """Find the configuration overlay that has the slurmaccounting role.
    
    Returns:
        Name of the configuration overlay with slurmaccounting role
        
    Raises:
        RuntimeError if no overlay found with slurmaccounting role
    """
    # List all overlays and their roles
    cmsh_cmd = "configurationoverlay\nlist\nquit\n"
    result = run_cmsh(cmsh_cmd)
    
    # Parse output to find overlay with slurmaccounting role
    # Format: "Name (key)  Priority  All head nodes  Nodes  Categories  Roles"
    overlay_name = None
    
    for line in result.stdout.split('\n'):
        line = line.strip()
        if not line or line.startswith('Name') or line.startswith('-'):
            continue
        
        # Check if this line contains "slurmaccounting" in the Roles column
        if 'slurmaccounting' in line.lower():
            # First column is the overlay name
            parts = line.split()
            if parts:
                overlay_name = parts[0]
                break
    
    if not overlay_name:
        raise RuntimeError(
            "Could not find a configuration overlay with the slurmaccounting role. "
            "Please verify your BCM Slurm configuration."
        )
    
    return overlay_name


def update_bcm_configuration(primary_headnode: str, skip_confirm: bool = False) -> bool:
    """Update BCM configuration to move slurm accounting to head nodes.
    
    This function:
    1. Finds the configuration overlay with slurmaccounting role
    2. Updates the slurmaccounting role:
       - Sets 'primary' to the primary BCM head node
       - Sets 'storagehost' to 'master' (BCM HA virtual hostname for active head)
    3. Updates the configuration overlay:
       - Clears the 'nodes' setting
       - Sets 'allheadnodes' to yes
    
    Args:
        primary_headnode: Hostname of the primary BCM head node
        skip_confirm: If True, don't prompt for confirmation
        
    Returns:
        True if configuration was updated successfully
    """
    print(f"\n{'=' * 65}")
    print("UPDATING BCM CONFIGURATION VIA CMSH")
    print('=' * 65)
    
    # Find the overlay
    print("\nFinding configuration overlay with slurmaccounting role...")
    overlay_name = find_slurmaccounting_overlay()
    print(f"  Found overlay: {overlay_name}")
    
    # Show current configuration
    print(f"\nCurrent configuration:")
    cmsh_show = f"""configurationoverlay
use {overlay_name}
show
roles
use slurmaccounting
show
quit
"""
    result = run_cmsh(cmsh_show, check=False)
    
    # Parse and display relevant settings
    current_nodes = ""
    current_allheadnodes = ""
    current_primary = ""
    current_storagehost = ""
    
    in_overlay = False
    in_role = False
    
    for line in result.stdout.split('\n'):
        line_lower = line.lower().strip()
        
        if 'nodes' in line_lower and 'all head nodes' not in line_lower:
            if 'slurmctl' in line_lower or ',' in line:
                current_nodes = line.split()[-1] if line.split() else ""
        if 'all head nodes' in line_lower:
            current_allheadnodes = line.split()[-1] if line.split() else ""
        if 'primary' in line_lower and 'primary' == line_lower.split()[0] if line_lower.split() else False:
            parts = line.split()
            if len(parts) >= 2:
                current_primary = parts[-1]
        if 'storagehost' in line_lower:
            parts = line.split()
            if len(parts) >= 2:
                current_storagehost = parts[-1]
    
    print(f"  Overlay: {overlay_name}")
    print(f"    Current Nodes: {current_nodes if current_nodes else '(none)'}")
    print(f"    Current All head nodes: {current_allheadnodes}")
    print(f"  Role: slurmaccounting")
    print(f"    Current primary: {current_primary}")
    print(f"    Current storagehost: {current_storagehost}")
    
    # Show planned changes
    print(f"\nPlanned changes:")
    print(f"  Overlay: {overlay_name}")
    print(f"    nodes         : (will be cleared)")
    print(f"    allheadnodes  : yes")
    print(f"  Role: slurmaccounting")
    print(f"    primary       : {primary_headnode}")
    print(f"    storagehost   : master  (BCM HA virtual hostname)")
    
    if not skip_confirm:
        answer = input("\nApply these BCM configuration changes? [y/N]: ").strip().lower()
        if answer not in ('y', 'yes'):
            print("Skipping BCM configuration update.")
            return False
    
    # Build cmsh commands to update configuration
    # Note: storagehost is set to "master" which is the BCM HA virtual hostname
    # that always points to the active head node
    cmsh_update = f"""configurationoverlay
use {overlay_name}
roles
use slurmaccounting
set primary {primary_headnode}
set storagehost master
commit
exit
set nodes
set allheadnodes yes
commit
quit
"""
    
    print("\nApplying BCM configuration changes...")
    try:
        result = run_cmsh(cmsh_update)
        print(f"  ✓ Updated slurmaccounting role: primary={primary_headnode}, storagehost=master")
        print(f"  ✓ Updated overlay: allheadnodes=yes, nodes cleared")
        return True
    except Exception as e:
        print(f"  ✗ Failed to update BCM configuration: {e}")
        return False


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


def restart_slurmdbd_services():
    """Restart slurmdbd services on head nodes after configuration change."""
    print("\nRestarting slurmdbd services on head nodes...")
    
    # Use cmsh to restart slurmdbd on all nodes with the slurmaccounting role
    cmsh_restart = """device
foreach -r slurmaccounting (services; restart slurmdbd)
quit
"""
    try:
        result = run_cmsh(cmsh_restart, check=False)
        print("  ✓ Sent restart command to slurmdbd on all accounting nodes")
        return True
    except Exception as e:
        print(f"  ⚠ Could not restart slurmdbd automatically: {e}")
        print("    Please manually restart slurmdbd: systemctl restart slurmdbd")
        return False


def main():
    ensure_root()

    print("=" * 65)
    print("SLURM ACCOUNTING DATABASE MIGRATION TO BCM HEAD NODES")
    print("=" * 65)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Find slurmdbd.conf
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

    # Get local hostname and determine primary BCM head node
    local_hostname = run_cmd(["hostname", "-s"], capture_output=True).stdout.strip()
    primary_headnode = get_primary_bcm_headnode()

    print("\nCurrent Slurm accounting DB configuration (from slurmdbd.conf):")
    print(f"  StorageHost : {cfg['storage_host']}")
    print(f"  StoragePort : {cfg['storage_port']}")
    print(f"  StorageLoc  : {cfg['storage_loc']}")
    print(f"  StorageUser : {cfg['storage_user']}")
    
    print(f"\nBCM Head Node Information:")
    print(f"  Local hostname      : {local_hostname}")
    print(f"  Primary head node   : {primary_headnode}")
    
    print(
        "\nThis script will:\n"
        f"  1) Dump the Slurm accounting DB from current StorageHost ({cfg['storage_host']})\n"
        f"  2) Import that dump into local MariaDB/MySQL on this head node ({local_hostname})\n"
        "  3) Grant the Slurm DB user access to the local database\n"
        "  4) Update BCM configuration via cmsh:\n"
        f"     - Set slurmaccounting role's primary to {primary_headnode}\n"
        "     - Set slurmaccounting role's storagehost to 'master' (BCM HA virtual hostname)\n"
        "     - Update overlay to use 'allheadnodes yes' (remove specific node assignments)\n"
        "  5) Restart slurmdbd services on head nodes\n"
        "\nAfter migration, the Slurm accounting database will be hosted on the BCM\n"
        "head nodes with HA provided by BCM's MySQL replication (cmha).\n"
    )

    answer = input("Proceed with migration? [y/N]: ").strip().lower()
    if answer not in ("y", "yes"):
        print("Aborting at user request.")
        sys.exit(0)

    # Suggest that Slurm services be stopped or quiesced first
    print(
        "\n" + "=" * 65 + "\n"
        "IMPORTANT: For a consistent snapshot, you should stop slurmdbd on the\n"
        f"current StorageHost ({cfg['storage_host']}) before proceeding.\n"
        "\nYou can do this with:\n"
        f"  ssh {cfg['storage_host']} systemctl stop slurmdbd\n"
        "=" * 65
    )
    
    answer = input("\nHave you stopped slurmdbd or is it safe to proceed? [y/N]: ").strip().lower()
    if answer not in ("y", "yes"):
        print("Aborting. Please stop slurmdbd and run this script again.")
        sys.exit(0)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    dump_dir = Path("/root/slurm-db-migration")
    dump_path = dump_dir / f"slurm_acct_db-{ts}.sql"

    # Step 0: Ensure database connectivity
    print(f"\n{'=' * 65}")
    print("CHECKING DATABASE CONNECTIVITY")
    print('=' * 65)
    
    if not ensure_db_connectivity(cfg):
        print("\nERROR: Cannot establish database connectivity. Aborting.", file=sys.stderr)
        sys.exit(1)

    # Step 1-3: Database migration
    print(f"\n{'=' * 65}")
    print("STEP 1-3: DATABASE MIGRATION")
    print('=' * 65)
    
    try:
        dump_remote_slurm_db(cfg, dump_path)
        import_db_to_local(cfg, dump_path)
    except Exception as e:
        print(f"\nERROR during database migration: {e}", file=sys.stderr)
        print(f"Dump file (if created) is at: {dump_path}", file=sys.stderr)
        sys.exit(1)
    
    print(f"\n✓ Database migration completed. Dump preserved at: {dump_path}")

    # Step 4: Update BCM configuration
    bcm_updated = update_bcm_configuration(primary_headnode, skip_confirm=False)
    
    # Step 5: Restart slurmdbd
    if bcm_updated:
        restart_slurmdbd_services()

    # Final summary
    print(f"\n{'=' * 65}")
    print("MIGRATION SUMMARY")
    print('=' * 65)
    
    print(f"\n✓ Database migrated from {cfg['storage_host']} to {local_hostname}")
    print(f"  Dump file: {dump_path}")
    
    if bcm_updated:
        print(f"\n✓ BCM configuration updated:")
        print(f"    slurmaccounting primary: {primary_headnode}")
        print(f"    slurmaccounting storagehost: master")
        print(f"    overlay allheadnodes: yes")
    else:
        print(f"\n⚠ BCM configuration was NOT updated automatically.")
        print("  You must manually update via cmsh:")
        print(f"    cmsh -c 'configurationoverlay; use <overlay>; roles; use slurmaccounting; "
              f"set primary {primary_headnode}; set storagehost master; commit; "
              f"exit; set nodes; set allheadnodes yes; commit'")
    
    print("\nNext steps:")
    print("  1) Verify MySQL HA is healthy:")
    print("     cmha status")
    print("  2) Verify slurmdbd is running on head nodes:")
    print("     systemctl status slurmdbd")
    print("  3) Test Slurm accounting:")
    print("     sacctmgr show cluster")
    print("     sacctmgr show account")
    print("  4) If using HA, sync the database to the passive head node:")
    print("     cmha dbreclone <passive-head-node>")
    
    print(f"\n{'=' * 65}")


if __name__ == "__main__":
    main()


