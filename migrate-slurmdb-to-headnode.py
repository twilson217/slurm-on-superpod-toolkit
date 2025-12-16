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
  5. Ensure slurmdbd systemd drop-in file exists on both head nodes
     (clears ConditionPathExists check that would prevent service start)

After running this script:
  - The Slurm accounting database will be on the BCM head nodes
  - BCM's MySQL HA (via cmha) will provide database redundancy
  - slurmdbd will run on the head nodes instead of dedicated controllers

High availability (HA) for the DB on the head nodes is provided by the
existing BCM HA MySQL replication configured by cmha-setup.

Options:
  --reupdate-primary    Re-run only the cmdaemon database update for primary
  --rollback            Rollback migration to original Slurm controllers
"""

import argparse
import os
import sys
import subprocess
import re
import time
import threading
import getpass
from datetime import datetime
from pathlib import Path


def confirm_prompt(prompt: str, default_yes: bool = False) -> bool:
    """Prompt user for confirmation with robust input handling.
    
    Args:
        prompt: The prompt message (should end with space, e.g., "Proceed? [Y/n]: ")
        default_yes: If True, empty input (just Enter) is treated as 'yes'
        
    Returns:
        True if user confirmed, False otherwise
    """
    try:
        answer = input(prompt).strip().lower()
    except EOFError:
        print("No input received (EOF).")
        return False
    
    if not answer:
        # Empty input - use default
        if default_yes:
            return True
        else:
            print("Empty input received. (Tip: Type 'y' before pressing Enter)")
            return False
    
    if answer in ('y', 'yes'):
        return True
    elif answer in ('n', 'no'):
        return False
    else:
        print(f"Input '{answer}' not recognized.")
        return default_yes  # Fall back to default on unrecognized input


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


def check_active_headnode() -> bool:
    """Check if this script is running on the active BCM head node.
    
    This is important because the cmdaemon database updates only take effect
    on the active head node. If running on the passive node, the changes
    will be overwritten when cmha syncs the database.
    
    Returns:
        True if on active head node (or HA not configured), False otherwise
    """
    # Get local hostname
    result = subprocess.run(["hostname", "-s"], capture_output=True, text=True)
    local_hostname = result.stdout.strip() if result.returncode == 0 else ""
    
    # Check cmha status
    result = subprocess.run(["cmha", "status"], capture_output=True, text=True)
    
    if result.returncode != 0:
        # cmha not available - likely single head node, OK to proceed
        return True
    
    # Parse output for active node (marked with *)
    active_node = None
    for line in result.stdout.split('\n'):
        if '->' in line and '*' in line:
            # Format: "hostname* -> ..." - the one with * is active
            match = re.search(r'(\S+)\*\s*->', line)
            if match:
                active_node = match.group(1)
                break
    
    if active_node and active_node != local_hostname:
        print(f"\n⚠ WARNING: This script is running on {local_hostname}, but the")
        print(f"  ACTIVE head node is {active_node}.")
        print(f"\n  The cmdaemon database updates must be made on the active head node.")
        print(f"  Changes made here may be overwritten when cmha syncs the database.")
        print(f"\n  Options:")
        print(f"    1) SSH to {active_node} and run this script there")
        print(f"    2) Run 'cmha makeactive' on this node first")
        print(f"    3) Continue anyway (changes may not take effect)")
        
        answer = input("\n  Continue anyway? [y/N]: ").strip().lower()
        if answer not in ('y', 'yes'):
            print("Aborting. Please run on the active head node.")
            sys.exit(0)
        return False
    
    return True


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


def _parse_cmd_conf_db_creds(cmd_conf_path: str = "/cm/local/apps/cmd/etc/cmd.conf") -> dict:
    """Parse BCM cmd.conf for local DB credentials.

    We use DBUser/DBPass here because on many BCM systems local MariaDB does not
    allow passwordless root via socket, and the cmdaemon DB user has sufficient
    privileges for schema/user management.
    """
    creds = {"user": None, "pass": None}
    if not os.path.exists(cmd_conf_path):
        return creds
    try:
        # Example lines:
        # DBUser = "cmdaemon"
        # DBPass = "secret"
        user_re = re.compile(r'^\s*DBUser\s*=\s*"([^"]*)"\s*$')
        pass_re = re.compile(r'^\s*DBPass\s*=\s*"([^"]*)"\s*$')
        with open(cmd_conf_path, "r") as f:
            for line in f:
                m = user_re.match(line)
                if m:
                    creds["user"] = m.group(1)
                    continue
                m = pass_re.match(line)
                if m:
                    creds["pass"] = m.group(1)
                    continue
    except Exception:
        # Best-effort: caller can still attempt passwordless auth
        return creds
    return creds


def _local_mysql_base_args(socket_path: str | None = None) -> list:
    """Build base mysql CLI args for local MariaDB/MySQL, including auth if available."""
    mysql_base = ["mysql"]
    if socket_path:
        mysql_base.extend(["--socket", socket_path])

    creds = _parse_cmd_conf_db_creds()
    if creds.get("user"):
        mysql_base.extend(["-u", creds["user"]])
    if creds.get("pass"):
        # Note: passing password on CLI can be visible to process listing.
        # This is consistent with existing script patterns for remote DB access.
        mysql_base.append(f"-p{creds['pass']}")
    return mysql_base


def _local_mysql_admin_base_args(socket_path: str | None = None) -> list:
    """Build mysql CLI args for privileged local operations (GRANT/ALTER USER).

    We try, in order:
      1) Debian/Ubuntu maintenance creds in /etc/mysql/debian.cnf (if they have GRANT OPTION)
      2) MySQL root over socket with no password (rare)
      3) Prompt for MySQL root password and use it over socket
    """
    debian_defaults = "/etc/mysql/debian.cnf"
    if os.path.exists(debian_defaults):
        # Only use if it can actually GRANT (requires GRANT OPTION). If not, we
        # fall back to root so we don't fail mid-migration.
        probe = subprocess.run(
            ["mysql", f"--defaults-file={debian_defaults}", "-N", "-e", "SHOW GRANTS FOR CURRENT_USER();"],
            capture_output=True, text=True
        )
        if probe.returncode == 0 and "WITH GRANT OPTION" in (probe.stdout or ""):
            return ["mysql", f"--defaults-file={debian_defaults}"]

    # Try root with no password first (socket auth)
    mysql_base = ["mysql"]
    if socket_path:
        mysql_base.extend(["--socket", socket_path])
    probe_root = subprocess.run(
        mysql_base + ["-u", "root", "-e", "SELECT 1;"],
        capture_output=True, text=True
    )
    if probe_root.returncode == 0:
        return mysql_base + ["-u", "root"]

    # Prompt for root password (interactive run)
    root_pw = getpass.getpass("Enter local MySQL root password (for GRANT/ALTER USER): ")
    if not root_pw:
        raise RuntimeError(
            "No MySQL root password provided. Cannot perform GRANT/ALTER USER on local DB."
        )
    return mysql_base + ["-u", "root", f"-p{root_pw}"]


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
    # We use socket auth as root to update the user permissions.
    #
    # Note: mysqldump with --routines requires SHOW ROUTINE privilege to run
    # SHOW CREATE PROCEDURE/FUNCTION. GRANT ALL on db.* does NOT reliably include
    # SHOW ROUTINE (often treated as global), so we grant it explicitly.
    grant_sql = (
        f"GRANT ALL PRIVILEGES ON `{storage_loc}`.* TO '{storage_user}'@'%' "
        f"IDENTIFIED BY '{storage_pass}'; "
        f"GRANT SHOW ROUTINE ON *.* TO '{storage_user}'@'%'; "
        f"FLUSH PRIVILEGES;"
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
            f"GRANT SHOW ROUTINE ON *.* TO '{storage_user}'@'%'; "
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
        if not confirm_prompt(f"\n  Update database permissions on {storage_host} to allow connections from this host? [Y/n]: ", default_yes=True):
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


def test_dump_privileges(cfg) -> tuple:
    """Test whether the configured DB user can dump routines (SHOW CREATE PROCEDURE/FUNCTION).

    Returns:
        (success: bool, error_message: str)
    """
    storage_host = cfg["storage_host"]
    storage_user = cfg["storage_user"]
    storage_pass = cfg["storage_pass"]
    storage_loc = cfg["storage_loc"]

    # Find one procedure name (Slurm typically has procedures, e.g., get_coord_qos)
    find_proc_sql = (
        "SELECT ROUTINE_NAME FROM information_schema.routines "
        f"WHERE ROUTINE_SCHEMA='{storage_loc}' AND ROUTINE_TYPE='PROCEDURE' "
        "LIMIT 1;"
    )
    find_cmd = [
        "mysql",
        "-h", storage_host,
        "-u", storage_user,
        f"-p{storage_pass}",
        "-N",
        "-e", find_proc_sql,
    ]
    result = subprocess.run(find_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return (False, result.stderr.strip() or "Failed to query information_schema.routines")

    proc_name = result.stdout.strip()
    if not proc_name:
        # No procedures found; dumping routines should be a no-op.
        return (True, "")

    # Try SHOW CREATE PROCEDURE on the first procedure we find
    show_sql = f"SHOW CREATE PROCEDURE `{proc_name}`;"
    show_cmd = [
        "mysql",
        "-h", storage_host,
        "-u", storage_user,
        f"-p{storage_pass}",
        storage_loc,
        "-e", show_sql,
    ]
    result = subprocess.run(show_cmd, capture_output=True, text=True)
    if result.returncode == 0:
        return (True, "")

    return (False, result.stderr.strip() or f"Failed to run SHOW CREATE PROCEDURE {proc_name}")


def ensure_dump_privileges(cfg) -> bool:
    """Ensure the configured DB user can dump routines; attempt to fix if not.

    This prevents mysqldump failures like:
      "insufficient privileges to SHOW CREATE PROCEDURE ..."
    """
    storage_host = cfg["storage_host"]
    storage_user = cfg["storage_user"]

    print(f"\nChecking dump privileges (routines) on {storage_host}...")
    ok, err = test_dump_privileges(cfg)
    if ok:
        print("  ✓ Routines dump privileges look OK")
        return True

    print(f"  ⚠ Routines dump privilege check failed: {err}")
    print(f"  The DB user '{storage_user}' likely lacks SHOW ROUTINE privilege.")
    print(f"  Attempting to fix this via SSH to {storage_host} (socket auth as root)...")

    mysql_available, mysql_path = check_remote_mysql_client(storage_host)
    if not mysql_available:
        print(f"  ✗ MySQL client not found on {storage_host}; cannot auto-fix privileges.")
        print(f"    Install a mysql client on {storage_host} and re-run, or grant manually:")
        print(f"      GRANT SHOW ROUTINE ON *.* TO '{storage_user}'@'%'; FLUSH PRIVILEGES;")
        return False

    if not confirm_prompt(f"\n  Grant SHOW ROUTINE (and DB privileges) to '{storage_user}'@'%' on {storage_host}? [Y/n]: ", default_yes=True):
        print("  Aborting. Please grant privileges manually and re-run.")
        return False

    if not fix_remote_db_permissions(cfg, mysql_path=mysql_path):
        return False

    print("\nRe-testing dump privileges...")
    ok, err = test_dump_privileges(cfg)
    if ok:
        print("  ✓ Routines dump privileges OK after update")
        return True

    print(f"  ✗ Still failing routines privilege check: {err}")
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


def get_bcm_headnodes() -> tuple:
    """Get both BCM head node hostnames (primary, secondary).
    
    Uses cmha status to determine which head node is currently active.
    Returns a tuple of (primary_hostname, secondary_hostname).
    If only one head node found, secondary will be None.
    """
    primary = None
    secondary = None
    
    # Try cmha status first
    result = subprocess.run(
        ["cmha", "status"],
        capture_output=True,
        text=True,
    )
    
    if result.returncode == 0:
        # Parse output for both nodes
        # Format: "basecm11* -> head2" - the one with * is active (primary)
        for line in result.stdout.split('\n'):
            if '->' in line:
                # Extract hostname (before the ->)
                match = re.search(r'(\S+?)(\*)?\s*->', line)
                if match:
                    hostname = match.group(1)
                    is_active = match.group(2) == '*'
                    if is_active:
                        primary = hostname
                    else:
                        secondary = hostname
    
    # Fallback to local hostname for primary if not found
    if not primary:
        result = run_cmd(["hostname", "-s"], capture_output=True)
        primary = result.stdout.strip()
    
    return (primary, secondary)


def get_primary_bcm_headnode() -> str:
    """Get the primary (active) BCM head node hostname.
    
    Uses cmha status to determine which head node is currently active.
    Falls back to local hostname if cmha is not available.
    """
    primary, _ = get_bcm_headnodes()
    return primary


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
    print(f"    Current primaryaccountingserver: {current_primary}")
    print(f"    Current storagehost: {current_storagehost}")
    
    # Show planned changes
    print(f"\nPlanned changes:")
    print(f"  Overlay: {overlay_name}")
    print(f"    nodes         : (will be cleared)")
    print(f"    allheadnodes  : yes")
    print(f"  Role: slurmaccounting")
    print(f"    primaryaccountingserver: {primary_headnode}")
    print(f"    storagehost   : master  (BCM HA virtual hostname)")
    
    if not skip_confirm:
        if not confirm_prompt("\nApply these BCM configuration changes? [Y/n]: ", default_yes=True):
            print("Skipping BCM configuration update.")
            return False
    
    # Build cmsh commands to update configuration
    # Note: storagehost is set to "master" which is the BCM HA virtual hostname
    # that always points to the active head node
    # Use cmsh -c with semicolons for cleaner execution
    # Parameter names from BCM admin manual:
    #   primaryaccountingserver - sets DbdHost (which node is primary)
    #   storagehost - sets StorageHost (MySQL server)
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    
    # Update slurmaccounting role settings
    role_cmd = (f"configurationoverlay; use {overlay_name}; roles; use slurmaccounting; "
                f"set primaryaccountingserver {primary_headnode}; set storagehost master; commit")
    
    # Update overlay settings (run on all head nodes, clear specific node assignments)
    overlay_cmd = f"configurationoverlay; use {overlay_name}; set nodes; set allheadnodes yes; commit"
    
    print("\nApplying BCM configuration changes...")
    try:
        # Update role via cmsh (storagehost)
        result = subprocess.run([cmsh_path, '-c', role_cmd], capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  ⚠ cmsh role update returned non-zero (may be expected for primaryaccountingserver)")
        print(f"  ✓ Updated slurmaccounting role: storagehost=master")
        
        # Update overlay
        result = subprocess.run([cmsh_path, '-c', overlay_cmd], capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Overlay update failed: {result.stderr}")
        print(f"  ✓ Updated overlay: allheadnodes=yes, nodes cleared")
        
        # Update primary directly in cmdaemon database
        # The 'primary' field is stored as JSON in the Roles table's extra_values column
        # and is not settable via cmsh
        #
        # IMPORTANT: We must STOP cmdaemon before updating the database, then START it.
        # If we update while cmdaemon is running and then restart, cmdaemon may overwrite
        # our database change with its cached in-memory state.
        print(f"\n  Stopping cmdaemon before database update...")
        result = subprocess.run(
            ["systemctl", "stop", "cmd"],
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            print(f"  ✓ cmdaemon stopped")
            time.sleep(2)  # Give cmdaemon time to fully stop and flush state
        else:
            print(f"  ⚠ Warning: Could not stop cmdaemon: {result.stderr}")
            print(f"    Proceeding with database update anyway...")
        
        print(f"  Updating slurmaccounting primary in cmdaemon database...")
        update_sql = (
            f"UPDATE Roles SET extra_values='{{\"ha\":true,\"primary\":\"{primary_headnode}\"}}' "
            f"WHERE CAST(name AS CHAR)='slurmaccounting'"
        )
        result = subprocess.run(
            ["mysql", "cmdaemon", "-e", update_sql],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"  ⚠ Warning: Could not update primary in database: {result.stderr}")
        else:
            print(f"  ✓ Updated slurmaccounting primary={primary_headnode} in cmdaemon database")
        
        # Verify the update was applied
        verify_sql = "SELECT CAST(extra_values AS CHAR) FROM Roles WHERE CAST(name AS CHAR)='slurmaccounting'"
        result = subprocess.run(
            ["mysql", "-N", "cmdaemon", "-e", verify_sql],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            current_value = result.stdout.strip()
            if primary_headnode in current_value:
                print(f"  ✓ Verified: {current_value}")
            else:
                print(f"  ⚠ Warning: Database shows unexpected value: {current_value}")
        
        # Start cmdaemon to pick up the database change
        print(f"  Starting cmdaemon...")
        result = subprocess.run(
            ["systemctl", "start", "cmd"],
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            print(f"  ✓ cmdaemon started")
            time.sleep(5)  # Give cmdaemon time to fully start
        else:
            print(f"  ⚠ Warning: Could not start cmdaemon: {result.stderr}")
        
        return True
    except Exception as e:
        print(f"  ✗ Failed to update BCM configuration: {e}")
        return False


def update_slurm_conf(primary_headnode: str, secondary_headnode: str, skip_confirm: bool = False) -> bool:
    """Check slurm.conf and clean up any duplicate AccountingStorageHost entries.
    
    This function checks if BCM's autogenerated section has the correct values.
    It removes any duplicate entries outside the autogenerated section to prevent
    duplicate warnings from slurmctld.
    
    Note: We do NOT add entries outside the autogenerated section because that
    causes duplicates when BCM eventually regenerates the file. The correct fix
    is to update the slurmaccounting 'primary' in the cmdaemon database.
    
    Args:
        primary_headnode: Hostname of the primary BCM head node (AccountingStorageHost)
        secondary_headnode: Hostname of the secondary BCM head node (AccountingStorageBackupHost)
        skip_confirm: If True, don't prompt for confirmation
        
    Returns:
        True if configuration is correct or was cleaned up successfully
    """
    slurm_conf_path = Path("/cm/shared/apps/slurm/var/etc/slurm/slurm.conf")
    
    if not slurm_conf_path.exists():
        print(f"  ✗ slurm.conf not found at {slurm_conf_path}")
        return False
    
    print(f"\n{'=' * 65}")
    print("CHECKING SLURM.CONF ACCOUNTING SETTINGS")
    print('=' * 65)
    
    try:
        # Read current content
        with open(slurm_conf_path, 'r') as f:
            lines = f.readlines()
        
        # Find the autogenerated section markers and check its contents
        autogen_start = None
        autogen_end = None
        autogen_has_host = False
        autogen_has_backup = False
        autogen_host_correct = False
        autogen_backup_correct = False
        autogen_host_value = None
        autogen_backup_value = None
        
        for i, line in enumerate(lines):
            if 'BEGIN AUTOGENERATED SECTION' in line:
                autogen_start = i
            elif 'END AUTOGENERATED SECTION' in line:
                autogen_end = i
                break
            elif autogen_start is not None:
                # We're inside the autogenerated section
                line_stripped = line.strip()
                if line_stripped.startswith('AccountingStorageHost='):
                    autogen_has_host = True
                    autogen_host_value = line_stripped.split('=', 1)[1]
                    if f'AccountingStorageHost={primary_headnode}' == line_stripped:
                        autogen_host_correct = True
                elif line_stripped.startswith('AccountingStorageBackupHost='):
                    autogen_has_backup = True
                    autogen_backup_value = line_stripped.split('=', 1)[1]
                    if secondary_headnode and f'AccountingStorageBackupHost={secondary_headnode}' == line_stripped:
                        autogen_backup_correct = True
        
        # Determine if BCM's autogenerated section has correct values
        bcm_handles_it = autogen_has_host and autogen_host_correct
        if secondary_headnode:
            bcm_handles_it = bcm_handles_it and autogen_has_backup and autogen_backup_correct
        
        if bcm_handles_it:
            print(f"\n  ✓ BCM autogenerated section has correct values:")
            print(f"    AccountingStorageHost={primary_headnode}")
            if secondary_headnode:
                print(f"    AccountingStorageBackupHost={secondary_headnode}")
        else:
            print(f"\n  ⚠ BCM autogenerated section does not have expected values:")
            if autogen_has_host:
                print(f"    AccountingStorageHost={autogen_host_value} (expected: {primary_headnode})")
            else:
                print(f"    AccountingStorageHost not found (expected: {primary_headnode})")
            if secondary_headnode:
                if autogen_has_backup:
                    print(f"    AccountingStorageBackupHost={autogen_backup_value} (expected: {secondary_headnode})")
                else:
                    print(f"    AccountingStorageBackupHost not found (expected: {secondary_headnode})")
            print(f"\n  The slurmaccounting 'primary' was updated in cmdaemon database.")
            print(f"  BCM should regenerate slurm.conf with correct values.")
            print(f"  If values are still wrong after cmdaemon restart, check Step 7 in the manual procedure.")
        
        # Count duplicates outside autogenerated section
        duplicates_outside = []
        for i, line in enumerate(lines):
            if autogen_start is not None and autogen_start <= i <= (autogen_end or len(lines)):
                continue  # Skip autogenerated section
            line_stripped = line.strip()
            if line_stripped.startswith('AccountingStorageHost=') or line_stripped.startswith('AccountingStorageBackupHost='):
                duplicates_outside.append((i, line_stripped))
        
        if duplicates_outside:
            print(f"\n  Found {len(duplicates_outside)} duplicate entries outside autogenerated section:")
            for line_num, content in duplicates_outside:
                print(f"    Line {line_num + 1}: {content}")
            
            if not skip_confirm:
                if not confirm_prompt("\nRemove these duplicates? [Y/n]: ", default_yes=True):
                    print("Skipping duplicate removal.")
                    return bcm_handles_it
            
            # Remove duplicates
            new_lines = []
            for i, line in enumerate(lines):
                if autogen_start is not None and autogen_start <= i <= (autogen_end or len(lines)):
                    new_lines.append(line)
                    continue
                line_stripped = line.strip()
                if line_stripped.startswith('AccountingStorageHost=') or line_stripped.startswith('AccountingStorageBackupHost='):
                    print(f"  Removing: {line_stripped}")
                    continue
                new_lines.append(line)
            
            with open(slurm_conf_path, 'w') as f:
                f.writelines(new_lines)
            print(f"  ✓ Removed duplicate entries from {slurm_conf_path}")
        else:
            print(f"\n  ✓ No duplicate entries found outside autogenerated section")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Failed to check/update slurm.conf: {e}")
        return False


def format_time(seconds: float) -> str:
    """Format seconds into MM:SS format."""
    minutes = int(seconds) // 60
    secs = int(seconds) % 60
    return f"{minutes:02d}:{secs:02d}"


def format_bytes(size: int) -> str:
    """Format bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def discover_slurmdbd_nodes() -> list:
    """Discover nodes that run slurmdbd via BCM slurmaccounting role.
    
    Uses 'device; foreach -l slurmaccounting' to find devices that have
    the slurmaccounting role assigned (directly or via overlay).
    
    Returns:
        List of node hostnames that run slurmdbd
    """
    nodes = []
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    
    if not os.path.exists(cmsh_path):
        print("  cmsh not found, cannot discover slurmdbd nodes")
        return nodes
    
    try:
        # Use foreach -l to find devices with slurmaccounting role (via overlay)
        result = subprocess.run(
            [cmsh_path, '-c', 'device; foreach -l slurmaccounting (get hostname)'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"  Could not query devices with slurmaccounting role: {result.stderr}")
            return nodes
        
        for line in result.stdout.strip().split('\n'):
            line = line.strip()
            if line:
                nodes.append(line)
        
        if nodes:
            print(f"  Found slurmdbd nodes: {', '.join(nodes)}")
        else:
            print(f"  ⚠ No devices found with slurmaccounting role")
        
    except Exception as e:
        print(f"  Warning: Could not discover slurmdbd nodes via cmsh: {e}")
    
    return nodes


def stop_slurmdbd_via_cmsh() -> bool:
    """Stop slurmdbd on all nodes with slurmaccounting role via cmsh.
    
    Using cmsh ensures BCM won't automatically restart the service.
    
    Returns:
        True if stop command succeeded, False otherwise
    """
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    
    try:
        result = subprocess.run(
            [cmsh_path, '-c', 'device; foreach -l slurmaccounting (services; stop slurmdbd)'],
            capture_output=True,
            text=True,
            timeout=60
        )
        return result.returncode == 0
    except Exception as e:
        print(f"  Error running cmsh stop command: {e}")
        return False


def prepare_for_migration(cfg) -> bool:
    """Prepare the source database for migration by stopping slurmdbd and killing connections.
    
    This ensures a consistent snapshot without blocking issues.
    
    Returns:
        True if ready to proceed, False if preparation failed
    """
    print(f"\n{'=' * 65}")
    print("PREPARING FOR MIGRATION")
    print('=' * 65)
    
    storage_host = cfg['storage_host']
    storage_user = cfg['storage_user']
    storage_pass = cfg['storage_pass']
    storage_loc = cfg['storage_loc']
    
    # Discover nodes that run slurmdbd
    print("\nDiscovering slurmdbd nodes via BCM...")
    slurmdbd_nodes = discover_slurmdbd_nodes()
    
    if not slurmdbd_nodes:
        print("  ⚠ Could not discover slurmdbd nodes from BCM")
        print("    You may need to manually stop slurmdbd before proceeding")
    
    # Check which nodes have slurmdbd running
    nodes_with_slurmdbd = []
    if slurmdbd_nodes:
        print("\nChecking slurmdbd status...")
        for node in slurmdbd_nodes:
            try:
                result = subprocess.run(
                    ['ssh', '-o', 'ConnectTimeout=5', '-o', 'StrictHostKeyChecking=no',
                     node, 'systemctl is-active slurmdbd'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0 and 'active' in result.stdout:
                    nodes_with_slurmdbd.append(node)
                    print(f"    {node}: slurmdbd is running")
                else:
                    print(f"    {node}: slurmdbd is stopped")
            except Exception as e:
                print(f"    {node}: could not check ({e})")
    
    # Stop slurmdbd if running - use cmsh to prevent BCM auto-restart
    if nodes_with_slurmdbd:
        print(f"\n  Found slurmdbd running on: {', '.join(nodes_with_slurmdbd)}")
        answer = input(f"  Stop slurmdbd via cmsh (prevents BCM auto-restart)? [Y/n]: ").strip().lower()
        if answer not in ('n', 'no'):
            print(f"    Stopping slurmdbd via cmsh...")
            if stop_slurmdbd_via_cmsh():
                print(f"    ✓ Stopped slurmdbd on all slurmaccounting nodes")
            else:
                print(f"    ⚠ cmsh stop command may have failed")
            # Give services time to fully stop
            time.sleep(2)
    else:
        print(f"\n  ✓ No slurmdbd services found running")
    
    # Check for blocking database connections on the source host
    print(f"\nChecking for blocking database connections on {storage_host}...")
    blocking_connections = []
    
    try:
        # Query processlist for connections to our database
        check_cmd = [
            'mysql',
            '-h', storage_host,
            '-u', storage_user,
            f"-p{storage_pass}",
            '-N', '-e',
            f"SELECT Id, User, Host, db, Command, Time FROM information_schema.processlist "
            f"WHERE db = '{storage_loc}' AND Command != 'Query' AND Id != CONNECTION_ID();"
        ]
        result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0 and result.stdout.strip():
            for line in result.stdout.strip().split('\n'):
                parts = line.split('\t')
                if len(parts) >= 4:
                    conn_id = parts[0]
                    conn_user = parts[1]
                    conn_host = parts[2]
                    conn_time = parts[5] if len(parts) > 5 else '0'
                    blocking_connections.append({
                        'id': conn_id,
                        'user': conn_user,
                        'host': conn_host,
                        'time': conn_time
                    })
    except Exception as e:
        print(f"  Warning: Could not check for blocking connections: {e}")
    
    # Kill blocking connections
    if blocking_connections:
        print(f"  Found {len(blocking_connections)} connection(s) that may block migration:")
        for conn in blocking_connections:
            print(f"    - ID {conn['id']}: {conn['user']}@{conn['host']} (idle {conn['time']}s)")
        
        answer = input(f"  Kill these connections to proceed? [Y/n]: ").strip().lower()
        if answer not in ('n', 'no'):
            for conn in blocking_connections:
                try:
                    kill_cmd = [
                        'mysql',
                        '-h', storage_host,
                        '-u', storage_user,
                        f"-p{storage_pass}",
                        '-e', f"KILL {conn['id']};"
                    ]
                    result = subprocess.run(kill_cmd, capture_output=True, text=True, timeout=10)
                    if result.returncode == 0:
                        print(f"    ✓ Killed connection {conn['id']}")
                    else:
                        # Connection may have already closed
                        print(f"    ⚠ Connection {conn['id']} already closed or could not kill")
                except Exception as e:
                    print(f"    ⚠ Error killing connection {conn['id']}: {e}")
            # Give a moment for connections to fully close
            time.sleep(1)
        else:
            print(f"\n  Warning: Migration may encounter lock issues.")
    else:
        print(f"  ✓ No blocking connections found")
    
    print(f"\n  ✓ Ready for migration")
    return True


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

    print(f"\nDumping Slurm accounting DB from {storage_host} ...")
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

    # Run mysqldump with progress indicator
    dump_complete = [False]
    dump_error = [None]
    start_time = time.time()
    
    def progress_reporter():
        spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        spin_idx = 0
        
        while not dump_complete[0]:
            elapsed = time.time() - start_time
            
            # Check current file size
            try:
                if dump_path.exists():
                    current_size = dump_path.stat().st_size
                    size_str = format_bytes(current_size)
                else:
                    size_str = "0 B"
            except:
                size_str = "..."
            
            status = f"\r  {spinner_chars[spin_idx]} Exporting... {format_time(elapsed)} elapsed | {size_str} written"
            sys.stdout.write(status)
            sys.stdout.flush()
            
            spin_idx = (spin_idx + 1) % len(spinner_chars)
            time.sleep(0.5)
        
        sys.stdout.write("\n")
        sys.stdout.flush()
    
    # Start progress thread
    progress_thread = threading.Thread(target=progress_reporter, daemon=True)
    progress_thread.start()
    
    try:
        with open(dump_path, "w") as out_f:
            result = subprocess.run(cmd, stdout=out_f, stderr=subprocess.PIPE, text=True)
        
        if result.returncode != 0:
            dump_error[0] = result.stderr
    except Exception as e:
        dump_error[0] = str(e)
    finally:
        dump_complete[0] = True
        progress_thread.join(timeout=2)
    
    if dump_error[0]:
        raise RuntimeError(
            f"mysqldump failed (host={storage_host}, db={storage_loc}):\n{dump_error[0]}"
        )

    final_size = dump_path.stat().st_size
    elapsed = time.time() - start_time
    print(f"  ✓ Dump completed: {format_bytes(final_size)} in {format_time(elapsed)}")
    print(f"    Saved to: {dump_path}")


def get_local_table_count(storage_loc: str, mysql_base: list) -> int:
    """Query the local database for the current number of tables."""
    try:
        query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{storage_loc}';"
        result = subprocess.run(
            mysql_base + ['-N', '-e', query],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0 and result.stdout.strip().isdigit():
            return int(result.stdout.strip())
    except:
        pass
    return -1


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
    # Use cmdaemon DB creds (from cmd.conf) for create/import. On BCM systems this
    # commonly works even when root socket auth is disabled.
    mysql_base = _local_mysql_base_args(socket_path if socket_path else None)

    print("\nCreating database on local MariaDB/MySQL ...")
    create_db_sql = (
        f"CREATE DATABASE IF NOT EXISTS `{storage_loc}` "
        f"DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    )
    run_cmd(mysql_base + ["-e", create_db_sql])

    # Get dump file size for display
    dump_size = dump_path.stat().st_size
    print(f"\nImporting dump into local database...")
    print(f"  Source file: {format_bytes(dump_size)}")
    
    # Run import with progress indicator
    import_complete = [False]
    import_error = [None]
    start_time = time.time()
    
    def progress_reporter():
        spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        spin_idx = 0
        
        while not import_complete[0]:
            elapsed = time.time() - start_time
            
            # Query table count for progress
            current_table_count = get_local_table_count(storage_loc, mysql_base)
            if current_table_count >= 0:
                table_str = f"| {current_table_count} tables"
            else:
                table_str = ""
            
            status = f"\r  {spinner_chars[spin_idx]} Importing... {format_time(elapsed)} elapsed {table_str}   "
            sys.stdout.write(status)
            sys.stdout.flush()
            
            spin_idx = (spin_idx + 1) % len(spinner_chars)
            time.sleep(1)
        
        sys.stdout.write("\n")
        sys.stdout.flush()
    
    # Start progress thread
    progress_thread = threading.Thread(target=progress_reporter, daemon=True)
    progress_thread.start()
    
    try:
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
            import_error[0] = result.stderr
    except Exception as e:
        import_error[0] = str(e)
    finally:
        import_complete[0] = True
        progress_thread.join(timeout=2)
    
    if import_error[0]:
        raise RuntimeError(
            f"mysql import failed into local DB {storage_loc}:\n{import_error[0]}"
        )
    
    elapsed = time.time() - start_time
    final_table_count = get_local_table_count(storage_loc, mysql_base)
    print(f"  ✓ Import completed: {final_table_count} tables in {format_time(elapsed)}")

    print("Granting privileges to Slurm DB user on local MariaDB/MySQL ...")
    mysql_admin_base = _local_mysql_admin_base_args(socket_path if socket_path else None)
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
        mysql_admin_base + ["-e", grant_sql_mariadb],
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
            mysql_admin_base + ["-e", grant_sql_mysql],
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
            run_cmd(mysql_admin_base + ["-e", grant_sql_simple])
    
    # Ensure the password is set correctly even if user already existed
    # This is critical when migrating to BCM head nodes where the slurm user
    # may already exist with a different password
    print("  Ensuring Slurm DB user password matches slurmdbd.conf on local node...")
    alter_sql = f"ALTER USER '{storage_user}'@'%' IDENTIFIED BY '{storage_pass}'; FLUSH PRIVILEGES;"
    result = subprocess.run(
        mysql_admin_base + ["-e", alter_sql],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode == 0:
        print(f"  ✓ Slurm DB user password updated on local node")
    else:
        print(f"  ⚠ Warning: Could not update password on local node: {result.stderr}")
        print(f"    You may need to manually run:")
        print(f"    mysql -e \"ALTER USER '{storage_user}'@'%' IDENTIFIED BY '<password>'; FLUSH PRIVILEGES;\"")
    
    # Also update password on the secondary head node (required for cmha dbreclone to work)
    _, secondary_headnode = get_bcm_headnodes()
    if secondary_headnode:
        print(f"  Ensuring Slurm DB user password matches on secondary node ({secondary_headnode})...")
        # Use ssh to run the ALTER USER on the secondary node.
        # Use BCM cmd.conf DB creds there as well (typically DBUser/DBPass = cmdaemon).
        remote_mysql = "mysql"
        # Prefer /etc/mysql/debian.cnf on the remote node if it exists, otherwise fall back
        # to cmd.conf DBUser/DBPass.
        remote_creds = _parse_cmd_conf_db_creds()
        remote_auth = ""
        remote_auth_fallback = ""
        if remote_creds.get("user"):
            remote_auth_fallback += f" -u {remote_creds['user']}"
        if remote_creds.get("pass"):
            remote_auth_fallback += f" -p{remote_creds['pass']}"
        remote_auth = f" --defaults-file=/etc/mysql/debian.cnf"
        ssh_cmd = [
            "ssh", secondary_headnode,
            f"bash -lc \"{remote_mysql}{remote_auth} -e \\\"ALTER USER '{storage_user}'@'%' IDENTIFIED BY '{storage_pass}'; FLUSH PRIVILEGES;\\\" || {remote_mysql}{remote_auth_fallback} -e \\\"ALTER USER '{storage_user}'@'%' IDENTIFIED BY '{storage_pass}'; FLUSH PRIVILEGES;\\\"\""
        ]
        result = subprocess.run(
            ssh_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            print(f"  ✓ Slurm DB user password updated on secondary node ({secondary_headnode})")
        else:
            print(f"  ⚠ Warning: Could not update password on {secondary_headnode}: {result.stderr}")
            print(f"    You may need to manually run on {secondary_headnode}:")
            print(f"    mysql -e \"ALTER USER '{storage_user}'@'%' IDENTIFIED BY '<password>'; FLUSH PRIVILEGES;\"")


def start_slurmdbd_services():
    """Start slurmdbd services on nodes with slurmaccounting role via cmsh."""
    print("\nStarting slurmdbd services...")
    
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    try:
        result = subprocess.run(
            [cmsh_path, '-c', 'device; foreach -l slurmaccounting (services; start slurmdbd)'],
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            print("  ✓ Started slurmdbd on all slurmaccounting nodes")
            return True
        else:
            print(f"  ⚠ Could not start slurmdbd: {result.stderr}")
            return False
    except Exception as e:
        print(f"  ⚠ Could not start slurmdbd automatically: {e}")
        print("    Manual start: cmsh -c \"device; foreach -l slurmaccounting (services; start slurmdbd)\"")
        return False


def ensure_slurmdbd_dropin(primary_headnode: str, secondary_headnode: str) -> bool:
    """Ensure the slurmdbd systemd drop-in file exists on both head nodes.
    
    The drop-in file clears the ConditionPathExists check that would otherwise
    prevent slurmdbd from starting (since the config is not at /etc/slurm/slurmdbd.conf
    but at /cm/shared/apps/slurm/var/etc/slurmdbd.conf).
    
    Args:
        primary_headnode: Hostname of the primary BCM head node
        secondary_headnode: Hostname of the secondary BCM head node (can be None)
        
    Returns:
        True if drop-in file exists/created on all head nodes
    """
    print(f"\n{'=' * 65}")
    print("CHECKING SLURMDBD SYSTEMD DROP-IN FILE")
    print('=' * 65)
    
    dropin_dir = "/etc/systemd/system/slurmdbd.service.d"
    dropin_file = f"{dropin_dir}/99-cmd.conf"
    dropin_content = """[Unit]
ConditionPathExists=
[Service]
Environment=SLURM_CONF=/cm/shared/apps/slurm/var/etc/slurm/slurm.conf
"""
    
    nodes_to_check = [primary_headnode]
    if secondary_headnode:
        nodes_to_check.append(secondary_headnode)
    
    local_hostname = subprocess.run(
        ["hostname", "-s"], capture_output=True, text=True
    ).stdout.strip()
    
    all_success = True
    
    for node in nodes_to_check:
        is_local = (node == local_hostname)
        print(f"\n  Checking {node}{'  (local)' if is_local else ''}...")
        
        # Check if drop-in file exists
        if is_local:
            file_exists = os.path.exists(dropin_file)
        else:
            result = subprocess.run(
                ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
                 node, f"test -f {dropin_file}"],
                capture_output=True, text=True, timeout=10
            )
            file_exists = (result.returncode == 0)
        
        if file_exists:
            print(f"    ✓ Drop-in file already exists: {dropin_file}")
            continue
        
        # Create the drop-in file
        print(f"    Creating drop-in file: {dropin_file}")
        
        try:
            if is_local:
                os.makedirs(dropin_dir, exist_ok=True)
                with open(dropin_file, 'w') as f:
                    f.write(dropin_content)
                # Reload systemd
                subprocess.run(["systemctl", "daemon-reload"], check=True)
                print(f"    ✓ Created drop-in file and reloaded systemd")
            else:
                # Create via SSH
                create_cmd = (
                    f"mkdir -p {dropin_dir} && "
                    f"cat > {dropin_file} << 'EOF'\n{dropin_content}EOF\n"
                    f"&& systemctl daemon-reload"
                )
                result = subprocess.run(
                    ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
                     node, create_cmd],
                    capture_output=True, text=True, timeout=30
                )
                if result.returncode == 0:
                    print(f"    ✓ Created drop-in file and reloaded systemd on {node}")
                else:
                    print(f"    ✗ Failed to create drop-in file on {node}: {result.stderr}")
                    all_success = False
        except Exception as e:
            print(f"    ✗ Error creating drop-in file on {node}: {e}")
            all_success = False
    
    return all_success


def reupdate_primary_only():
    """Re-run only the cmdaemon database update for the slurmaccounting primary.
    
    This is useful if the primary field was not properly updated during the
    initial migration, or if it was overwritten by some other process.
    """
    ensure_root()
    check_active_headnode()
    
    print("=" * 65)
    print("RE-UPDATE SLURMACCOUNTING PRIMARY")
    print("=" * 65)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Get current values
    primary_headnode, secondary_headnode = get_bcm_headnodes()
    
    print(f"BCM Head Node Information:")
    print(f"  Primary head node    : {primary_headnode}")
    print(f"  Secondary head node  : {secondary_headnode if secondary_headnode else '(none)'}")
    
    # Show current database value
    verify_sql = "SELECT CAST(extra_values AS CHAR) FROM Roles WHERE CAST(name AS CHAR)='slurmaccounting'"
    result = subprocess.run(
        ["mysql", "-N", "cmdaemon", "-e", verify_sql],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        current_value = result.stdout.strip()
        print(f"\nCurrent database value:")
        print(f"  {current_value}")
    
    print(f"\nThis will update the slurmaccounting primary to: {primary_headnode}")
    answer = input("Proceed? [y/N]: ").strip().lower()
    if answer not in ("y", "yes"):
        print("Aborting at user request.")
        sys.exit(0)
    
    # Stop cmdaemon
    print(f"\nStopping cmdaemon...")
    result = subprocess.run(
        ["systemctl", "stop", "cmd"],
        capture_output=True, text=True, timeout=60
    )
    if result.returncode == 0:
        print(f"  ✓ cmdaemon stopped")
        time.sleep(2)
    else:
        print(f"  ⚠ Warning: Could not stop cmdaemon: {result.stderr}")
    
    # Update database
    print(f"\nUpdating slurmaccounting primary in cmdaemon database...")
    update_sql = (
        f"UPDATE Roles SET extra_values='{{\"ha\":true,\"primary\":\"{primary_headnode}\"}}' "
        f"WHERE CAST(name AS CHAR)='slurmaccounting'"
    )
    result = subprocess.run(
        ["mysql", "cmdaemon", "-e", update_sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  ✗ Failed to update: {result.stderr}")
        # Try to start cmdaemon anyway
        subprocess.run(["systemctl", "start", "cmd"], timeout=60)
        sys.exit(1)
    
    print(f"  ✓ Updated slurmaccounting primary={primary_headnode}")
    
    # Verify
    result = subprocess.run(
        ["mysql", "-N", "cmdaemon", "-e", verify_sql],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        current_value = result.stdout.strip()
        if primary_headnode in current_value:
            print(f"  ✓ Verified: {current_value}")
        else:
            print(f"  ⚠ Warning: Unexpected value: {current_value}")
    
    # Start cmdaemon
    print(f"\nStarting cmdaemon...")
    result = subprocess.run(
        ["systemctl", "start", "cmd"],
        capture_output=True, text=True, timeout=60
    )
    if result.returncode == 0:
        print(f"  ✓ cmdaemon started")
        time.sleep(3)
    else:
        print(f"  ⚠ Warning: Could not start cmdaemon: {result.stderr}")
    
    # Verify via cmsh
    print(f"\nVerifying via cmsh...")
    try:
        cmsh_result = subprocess.run(
            ["/cm/local/apps/cmd/bin/cmsh", "-c",
             "configurationoverlay; use slurm-accounting; roles; use slurmaccounting; get primary"],
            capture_output=True, text=True, timeout=30
        )
        if cmsh_result.returncode == 0:
            print(f"  cmsh shows: {cmsh_result.stdout.strip()}")
    except Exception as e:
        print(f"  Could not verify via cmsh: {e}")
    
    # Also ensure drop-in files exist
    ensure_slurmdbd_dropin(primary_headnode, secondary_headnode)
    
    print(f"\n{'=' * 65}")
    print("RE-UPDATE COMPLETE")
    print('=' * 65)
    print("\nNext steps:")
    print("  1) Run 'cmha dbreclone <passive-node>' to sync cmdaemon database")
    print("  2) Restart slurmdbd: systemctl restart slurmdbd")


def rollback_migration(original_primary: str, original_backup: str = None):
    """Rollback the migration by updating BCM configuration to point back to original hosts.
    
    This updates the cmdaemon database to point the slurmaccounting primary back to
    the original Slurm controller. It does NOT restore the actual Slurm accounting
    database - that should still be intact on the original hosts.
    
    Args:
        original_primary: Hostname of the original primary Slurm controller (e.g., slurmctl-01)
        original_backup: Hostname of the original backup Slurm controller (optional)
    """
    ensure_root()
    check_active_headnode()
    
    print("=" * 65)
    print("ROLLBACK SLURM ACCOUNTING MIGRATION")
    print("=" * 65)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Get current BCM head nodes for reference
    primary_headnode, secondary_headnode = get_bcm_headnodes()
    
    print(f"Current BCM Head Nodes:")
    print(f"  Primary   : {primary_headnode}")
    print(f"  Secondary : {secondary_headnode if secondary_headnode else '(none)'}")
    
    print(f"\nRollback Target (original Slurm controllers):")
    print(f"  Primary   : {original_primary}")
    print(f"  Backup    : {original_backup if original_backup else '(none)'}")
    
    # Show current database value
    verify_sql = "SELECT CAST(extra_values AS CHAR) FROM Roles WHERE CAST(name AS CHAR)='slurmaccounting'"
    result = subprocess.run(
        ["mysql", "-N", "cmdaemon", "-e", verify_sql],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        current_value = result.stdout.strip()
        print(f"\nCurrent slurmaccounting extra_values:")
        print(f"  {current_value}")
    
    print(f"\n⚠ WARNING: This will revert BCM configuration to use the original Slurm controllers.")
    print("  The Slurm accounting database on those controllers should still be intact.")
    print("  This does NOT delete the migrated database from the BCM head nodes.")
    
    answer = input("\nProceed with rollback? [y/N]: ").strip().lower()
    if answer not in ("y", "yes"):
        print("Aborting rollback at user request.")
        sys.exit(0)
    
    # Stop slurmdbd on head nodes first
    print(f"\n{'=' * 65}")
    print("STOPPING SLURMDBD SERVICES")
    print('=' * 65)
    
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    try:
        result = subprocess.run(
            [cmsh_path, '-c', 'device; foreach -l slurmaccounting (services; stop slurmdbd)'],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            print("  ✓ Stopped slurmdbd on all slurmaccounting nodes")
        else:
            print(f"  ⚠ Could not stop slurmdbd via cmsh: {result.stderr}")
    except Exception as e:
        print(f"  ⚠ Error stopping slurmdbd: {e}")
    
    time.sleep(2)
    
    # Update cmdaemon database
    print(f"\n{'=' * 65}")
    print("UPDATING CMDAEMON DATABASE")
    print('=' * 65)
    
    print(f"\nStopping cmdaemon...")
    result = subprocess.run(
        ["systemctl", "stop", "cmd"],
        capture_output=True, text=True, timeout=60
    )
    if result.returncode == 0:
        print(f"  ✓ cmdaemon stopped")
        time.sleep(2)
    else:
        print(f"  ⚠ Warning: Could not stop cmdaemon: {result.stderr}")
    
    # Update slurmaccounting primary
    print(f"\nUpdating slurmaccounting primary to: {original_primary}")
    update_sql = (
        f"UPDATE Roles SET extra_values='{{\"ha\":true,\"primary\":\"{original_primary}\"}}' "
        f"WHERE CAST(name AS CHAR)='slurmaccounting'"
    )
    result = subprocess.run(
        ["mysql", "cmdaemon", "-e", update_sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  ✗ Failed to update primary: {result.stderr}")
    else:
        print(f"  ✓ Updated slurmaccounting primary={original_primary}")
    
    # Verify
    result = subprocess.run(
        ["mysql", "-N", "cmdaemon", "-e", verify_sql],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"  ✓ Verified: {result.stdout.strip()}")
    
    # Start cmdaemon
    print(f"\nStarting cmdaemon...")
    result = subprocess.run(
        ["systemctl", "start", "cmd"],
        capture_output=True, text=True, timeout=60
    )
    if result.returncode == 0:
        print(f"  ✓ cmdaemon started")
        time.sleep(5)
    else:
        print(f"  ⚠ Warning: Could not start cmdaemon: {result.stderr}")
    
    # Update BCM configuration via cmsh
    # Important: Update overlay nodes FIRST, then storagehost
    # If we try to set storagehost to a node that isn't in the overlay yet,
    # cmsh may fail or produce warnings
    print(f"\nUpdating BCM configuration via cmsh...")
    try:
        # Find the overlay name
        overlay_name = find_slurmaccounting_overlay()
        
        # Step 1: Update overlay nodes back to original controllers
        # This must be done BEFORE setting storagehost, otherwise cmsh may
        # reject the storagehost value since the node isn't in the overlay
        nodes_str = original_primary
        if original_backup:
            nodes_str = f"{original_primary},{original_backup}"
        
        overlay_cmd = (f"configurationoverlay; use {overlay_name}; "
                       f"set allheadnodes no; set nodes {nodes_str}; commit")
        result = subprocess.run(
            [cmsh_path, '-c', overlay_cmd],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            print(f"  ✓ Updated overlay nodes={nodes_str}")
        else:
            print(f"  ⚠ Could not update overlay nodes: {result.stderr}")
        
        # Step 2: Update storagehost back to the original (not 'master')
        # Note: The 'primary' field cannot be set via cmsh - it's in extra_values JSON
        role_cmd = (f"configurationoverlay; use {overlay_name}; roles; use slurmaccounting; "
                    f"set storagehost {original_primary}; commit")
        result = subprocess.run(
            [cmsh_path, '-c', role_cmd],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            print(f"  ✓ Updated storagehost={original_primary}")
        else:
            print(f"  ⚠ Could not update storagehost: {result.stderr}")
            
    except Exception as e:
        print(f"  ⚠ Error updating BCM configuration: {e}")
    
    # Final summary
    print(f"\n{'=' * 65}")
    print("ROLLBACK SUMMARY")
    print('=' * 65)
    
    print(f"\n✓ BCM configuration reverted to use original Slurm controllers:")
    print(f"    slurmaccounting primary: {original_primary}")
    print(f"    slurmaccounting storagehost: {original_primary}")
    
    print(f"\nNext steps:")
    print(f"  1) Verify original Slurm controllers are running:")
    print(f"       ssh {original_primary} 'systemctl status slurmdbd'")
    print(f"  2) Sync cmdaemon database to passive BCM head node:")
    print(f"       cmha dbreclone <passive-head-node>")
    print(f"  3) Test Slurm accounting:")
    print(f"       sacctmgr show cluster")
    
    print(f"\n{'=' * 65}")


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Migrate Slurm accounting database from Slurm controllers to BCM head nodes.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full migration
  %(prog)s

  # Re-update only the slurmaccounting primary field
  %(prog)s --reupdate-primary

  # Rollback to original Slurm controllers
  %(prog)s --rollback --original-primary slurmctl-01 --original-backup slurmctl-02
"""
    )
    
    parser.add_argument(
        '--reupdate-primary',
        action='store_true',
        help='Only re-run the cmdaemon database update for slurmaccounting primary'
    )
    
    parser.add_argument(
        '--rollback',
        action='store_true',
        help='Rollback migration to use original Slurm controllers'
    )
    
    parser.add_argument(
        '--original-primary',
        type=str,
        metavar='HOSTNAME',
        help='Original primary Slurm controller hostname (required for --rollback)'
    )
    
    parser.add_argument(
        '--original-backup',
        type=str,
        metavar='HOSTNAME',
        help='Original backup Slurm controller hostname (optional for --rollback)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.rollback and not args.original_primary:
        parser.error("--rollback requires --original-primary")
    
    if args.reupdate_primary and args.rollback:
        parser.error("Cannot use --reupdate-primary and --rollback together")
    
    return args


def main():
    args = parse_arguments()
    
    # Handle special modes first
    if args.reupdate_primary:
        reupdate_primary_only()
        return
    
    if args.rollback:
        rollback_migration(args.original_primary, args.original_backup)
        return
    
    # Normal migration flow
    ensure_root()
    check_active_headnode()

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

    # Get local hostname and determine BCM head nodes
    local_hostname = run_cmd(["hostname", "-s"], capture_output=True).stdout.strip()
    primary_headnode, secondary_headnode = get_bcm_headnodes()

    print("\nCurrent Slurm accounting DB configuration (from slurmdbd.conf):")
    print(f"  StorageHost : {cfg['storage_host']}")
    print(f"  StoragePort : {cfg['storage_port']}")
    print(f"  StorageLoc  : {cfg['storage_loc']}")
    print(f"  StorageUser : {cfg['storage_user']}")
    
    print(f"\nBCM Head Node Information:")
    print(f"  Local hostname       : {local_hostname}")
    print(f"  Primary head node    : {primary_headnode}")
    print(f"  Secondary head node  : {secondary_headnode if secondary_headnode else '(none)'}")
    
    print(
        "\nThis script will:\n"
        f"  1) Dump the Slurm accounting DB from current StorageHost ({cfg['storage_host']})\n"
        f"  2) Import that dump into local MariaDB/MySQL on this head node ({local_hostname})\n"
        "  3) Grant the Slurm DB user access to the local database\n"
        "  4) Update BCM configuration via cmsh:\n"
        "     - Set slurmaccounting role's storagehost to 'master' (BCM HA virtual hostname)\n"
        "     - Update overlay to use 'allheadnodes yes' (remove specific node assignments)\n"
        "  5) Update slurm.conf with correct accounting host settings:\n"
        f"     - AccountingStorageHost={primary_headnode}\n"
        f"     - AccountingStorageBackupHost={secondary_headnode if secondary_headnode else '(none)'}\n"
        "  6) Ensure slurmdbd systemd drop-in file on both head nodes\n"
        "     (clears ConditionPathExists check that would prevent service start)\n"
        "\nAfter migration, the Slurm accounting database will be hosted on the BCM\n"
        "head nodes with HA provided by BCM's MySQL replication (cmha).\n"
    )

    answer = input("Proceed with migration? [y/N]: ").strip().lower()
    if answer not in ("y", "yes"):
        print("Aborting at user request.")
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

    # Step 0.25: Ensure we can dump routines (SHOW CREATE PROCEDURE) before starting a long mysqldump
    if not ensure_dump_privileges(cfg):
        print("\nERROR: Insufficient privileges to dump routines. Aborting.", file=sys.stderr)
        sys.exit(1)

    # Step 0.5: Prepare for migration (stop slurmdbd, kill connections)
    if not prepare_for_migration(cfg):
        print("\nERROR: Could not prepare for migration. Aborting.", file=sys.stderr)
        sys.exit(1)

    # Step 1-3: Database migration
    print(f"\n{'=' * 65}")
    print("DATABASE MIGRATION")
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
    
    # Step 5: Update slurm.conf with correct accounting host settings
    # BCM's autogenerated section doesn't always set these correctly
    slurm_conf_updated = update_slurm_conf(primary_headnode, secondary_headnode, skip_confirm=False)
    
    # Step 6: Ensure slurmdbd systemd drop-in file exists on both head nodes
    # This clears the ConditionPathExists check that would otherwise prevent slurmdbd from starting
    dropin_ok = ensure_slurmdbd_dropin(primary_headnode, secondary_headnode)
    
    # Note: We do NOT auto-restart slurmdbd here because cmha dbreclone 
    # needs to run first to sync the database to the passive head node

    # Final summary
    print(f"\n{'=' * 65}")
    print("MIGRATION SUMMARY")
    print('=' * 65)
    
    print(f"\n✓ Database migrated from {cfg['storage_host']} to {local_hostname}")
    print(f"  Dump file: {dump_path}")
    
    if bcm_updated:
        print(f"\n✓ BCM configuration updated:")
        print(f"    slurmaccounting storagehost: master")
        print(f"    overlay allheadnodes: yes")
    else:
        print(f"\n⚠ BCM configuration was NOT updated automatically.")
        print("  You must manually update via cmsh:")
        print(f"    cmsh -c 'configurationoverlay; use <overlay>; roles; use slurmaccounting; "
              f"set storagehost master; commit'")
        print(f"    cmsh -c 'configurationoverlay; use <overlay>; set nodes; set allheadnodes yes; commit'")
    
    if slurm_conf_updated:
        print(f"\n✓ slurm.conf updated:")
        print(f"    AccountingStorageHost={primary_headnode}")
        if secondary_headnode:
            print(f"    AccountingStorageBackupHost={secondary_headnode}")
    else:
        print(f"\n⚠ slurm.conf was NOT updated automatically.")
        print("  You must manually edit /cm/shared/apps/slurm/var/etc/slurm/slurm.conf:")
        print(f"    AccountingStorageHost={primary_headnode}")
        if secondary_headnode:
            print(f"    AccountingStorageBackupHost={secondary_headnode}")
    
    if dropin_ok:
        print(f"\n✓ slurmdbd systemd drop-in file configured on head nodes")
        print(f"    /etc/systemd/system/slurmdbd.service.d/99-cmd.conf")
    else:
        print(f"\n⚠ slurmdbd systemd drop-in file may need manual setup.")
        print("  Create /etc/systemd/system/slurmdbd.service.d/99-cmd.conf with:")
        print("    [Unit]")
        print("    ConditionPathExists=")
        print("    [Service]")
        print("    Environment=SLURM_CONF=/cm/shared/apps/slurm/var/etc/slurm/slurm.conf")
    
    print(f"\nNext steps (in order):")
    print("  1) Verify MySQL HA is healthy:")
    print("     cmha status")
    print("  2) Sync database to the passive head node:")
    print("     cmha dbreclone <passive-head-node>")
    print("  3) Start slurmdbd services via cmsh:")
    print("     cmsh -c \"device; foreach -l slurmaccounting (services; start slurmdbd)\"")
    print("  4) Verify slurmdbd is running:")
    print("     systemctl status slurmdbd")
    print("  5) Test Slurm accounting:")
    print("     sacctmgr show cluster")
    print("     sacctmgr show account")
    
    print(f"\n{'=' * 65}")


if __name__ == "__main__":
    main()


