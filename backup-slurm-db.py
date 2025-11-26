#!/usr/bin/env python3
"""
Slurm Database Backup and Restore Script

Creates compressed backups of the Slurm accounting database (slurmdbd) and
can restore from those backups. Reads database credentials from slurmdbd.conf.

Usage:
    # Backup operations
    ./backup-slurm-db.py                    # Create backup in default directory
    ./backup-slurm-db.py -o /path/to/dir    # Create backup in specific directory
    ./backup-slurm-db.py --retention 7      # Keep only last 7 days of backups
    ./backup-slurm-db.py --no-compress      # Skip gzip compression
    
    # Restore operations
    ./backup-slurm-db.py --restore FILE     # Restore from backup file
    ./backup-slurm-db.py --restore FILE -y  # Restore without confirmation prompt
"""

import argparse
import gzip
import glob
import os
import re
import subprocess
import sys
import threading
import time
import configparser
from datetime import datetime, timedelta
from pathlib import Path

# Optional default backup directory for this script, useful for cron jobs.
# If you want a fixed default location when -o/--output-dir is NOT provided,
# uncomment the line below and set it to your preferred directory, e.g.:
#
# SLURM_DB_DEFAULT_BACKUP_DIR = "/var/spool/cmd/backup"
#
# When left commented out, the script will use values from healthcheck-config.conf
# or built-in fallback locations.
try:
    SLURM_DB_DEFAULT_BACKUP_DIR  # type: ignore[name-defined]
except NameError:
    SLURM_DB_DEFAULT_BACKUP_DIR = None


class Colors:
    """ANSI color codes"""
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    RESET = '\033[0m'
    
    @classmethod
    def disable(cls):
        """Disable colors for non-TTY output"""
        cls.GREEN = cls.YELLOW = cls.RED = cls.BLUE = cls.BOLD = cls.RESET = ''


class SlurmDatabaseBackup:
    """Slurm accounting database backup utility"""
    
    def __init__(self, output_dir: str = None, compress: bool = True, 
                 retention_days: int = None, verbose: bool = False):
        self.output_dir = output_dir
        self.compress = compress
        self.retention_days = retention_days
        self.verbose = verbose
        self.slurmdbd_conf_path = None
        self.db_config = {}
        
        if not sys.stdout.isatty():
            Colors.disable()
    
    def log(self, message: str, color: str = ''):
        """Print log message"""
        print(f"{color}{message}{Colors.RESET}")
    
    def log_verbose(self, message: str):
        """Print verbose log message"""
        if self.verbose:
            print(f"  {message}")
    
    def find_slurmdbd_conf(self) -> str:
        """Find slurmdbd.conf file"""
        possible_paths = [
            '/cm/shared/apps/slurm/var/etc/slurmdbd.conf',
            '/cm/shared/apps/slurm/current/etc/slurmdbd.conf',
            '/cm/local/apps/slurm/var/etc/slurmdbd.conf',
            '/cm/local/apps/slurm/current/etc/slurmdbd.conf',
            '/etc/slurm/slurmdbd.conf',
            '/usr/local/etc/slurmdbd.conf',
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                self.log_verbose(f"Found slurmdbd.conf at: {path}")
                return path
        
        return None
    
    def parse_slurmdbd_conf(self, conf_path: str) -> dict:
        """Parse slurmdbd.conf to extract database configuration"""
        config = {
            'storage_host': 'localhost',
            'storage_port': '3306',
            'storage_user': 'slurm',
            'storage_pass': None,
            'storage_loc': 'slurm_acct_db',
        }
        
        try:
            with open(conf_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                    
                    # Parse key=value pairs
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip().lower()
                        value = value.strip()
                        
                        if key == 'storagehost':
                            config['storage_host'] = value
                        elif key == 'storageport':
                            config['storage_port'] = value
                        elif key == 'storageuser':
                            config['storage_user'] = value
                        elif key == 'storagepass':
                            config['storage_pass'] = value
                        elif key == 'storageloc':
                            config['storage_loc'] = value
            
            return config
        
        except Exception as e:
            self.log(f"Error parsing slurmdbd.conf: {e}", Colors.RED)
            return None
    
    def get_backup_directory(self) -> str:
        """Determine backup directory"""
        if self.output_dir:
            return self.output_dir
        
        # Script-level default, intended for cron use when -o is not provided
        global SLURM_DB_DEFAULT_BACKUP_DIR
        if SLURM_DB_DEFAULT_BACKUP_DIR:
            return SLURM_DB_DEFAULT_BACKUP_DIR
        
        # Try to read from healthcheck config
        healthcheck_conf = '/root/slurm-upgrade/healthcheck-config.conf'
        if os.path.exists(healthcheck_conf):
            try:
                config = configparser.ConfigParser()
                config.read(healthcheck_conf)
                if config.has_option('accounting', 'slurm_db_backup_dir'):
                    backup_dir = config.get('accounting', 'slurm_db_backup_dir')
                    if backup_dir and os.path.exists(backup_dir):
                        return backup_dir
            except:
                pass
        
        # Default backup locations
        default_dirs = [
            '/var/spool/cmd/backup',
            '/backup/slurm',
            '/var/backup/slurm',
            '/tmp/slurm-backup',
        ]
        
        for dir_path in default_dirs:
            if os.path.exists(dir_path) and os.access(dir_path, os.W_OK):
                return dir_path
        
        # Fallback to /tmp
        return '/tmp'
    
    def create_backup(self) -> tuple[bool, str]:
        """Create database backup"""
        # Find and parse slurmdbd.conf
        self.slurmdbd_conf_path = self.find_slurmdbd_conf()
        
        if not self.slurmdbd_conf_path:
            self.log("ERROR: Could not find slurmdbd.conf", Colors.RED)
            self.log("Checked common locations in /cm/shared, /cm/local, /etc/slurm", Colors.RED)
            return False, None
        
        self.log(f"Reading database configuration from: {self.slurmdbd_conf_path}")
        
        self.db_config = self.parse_slurmdbd_conf(self.slurmdbd_conf_path)
        
        if not self.db_config:
            self.log("ERROR: Failed to parse slurmdbd.conf", Colors.RED)
            return False, None
        
        self.log_verbose(f"Database: {self.db_config['storage_loc']} on {self.db_config['storage_host']}")
        self.log_verbose(f"User: {self.db_config['storage_user']}")
        
        # Determine backup directory
        backup_dir = self.get_backup_directory()
        
        if not os.path.exists(backup_dir):
            self.log(f"Creating backup directory: {backup_dir}")
            try:
                os.makedirs(backup_dir, mode=0o755)
            except Exception as e:
                self.log(f"ERROR: Could not create directory: {e}", Colors.RED)
                return False, None
        
        # Generate backup filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f"slurm_acct_db_backup_{timestamp}.sql"
        
        if self.compress:
            backup_filename += ".gz"
        
        backup_path = os.path.join(backup_dir, backup_filename)
        
        self.log(f"\n{Colors.BOLD}Creating backup...{Colors.RESET}")
        self.log(f"Backup file: {backup_path}")
        
        # Build mysqldump command
        mysqldump_cmd = [
            'mysqldump',
            '--single-transaction',
            '--quick',
            '--lock-tables=false',
            f"--host={self.db_config['storage_host']}",
            f"--port={self.db_config['storage_port']}",
            f"--user={self.db_config['storage_user']}",
        ]
        
        # Add password if specified
        if self.db_config['storage_pass']:
            mysqldump_cmd.append(f"--password={self.db_config['storage_pass']}")
        
        # Add database name
        mysqldump_cmd.append(self.db_config['storage_loc'])
        
        # Execute backup
        try:
            if self.compress:
                # Pipe through gzip
                dump_process = subprocess.Popen(
                    mysqldump_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=False
                )
                
                with open(backup_path, 'wb') as f:
                    gzip_process = subprocess.Popen(
                        ['gzip', '-c'],
                        stdin=dump_process.stdout,
                        stdout=f,
                        stderr=subprocess.PIPE
                    )
                    
                    dump_process.stdout.close()
                    gzip_stderr = gzip_process.communicate()[1]
                    dump_stderr = dump_process.communicate()[1]
                    
                    if dump_process.returncode != 0:
                        error_msg = dump_stderr.decode() if dump_stderr else "Unknown error"
                        self.log(f"ERROR: mysqldump failed: {error_msg}", Colors.RED)
                        return False, None
                    
                    if gzip_process.returncode != 0:
                        error_msg = gzip_stderr.decode() if gzip_stderr else "Unknown error"
                        self.log(f"ERROR: gzip compression failed: {error_msg}", Colors.RED)
                        return False, None
            else:
                # Direct output to file
                with open(backup_path, 'w') as f:
                    result = subprocess.run(
                        mysqldump_cmd,
                        stdout=f,
                        stderr=subprocess.PIPE,
                        text=True
                    )
                    
                    if result.returncode != 0:
                        self.log(f"ERROR: mysqldump failed: {result.stderr}", Colors.RED)
                        return False, None
            
            # Verify backup was created
            if not os.path.exists(backup_path):
                self.log("ERROR: Backup file was not created", Colors.RED)
                return False, None
            
            backup_size = os.path.getsize(backup_path)
            
            if backup_size < 1024:
                self.log(f"WARNING: Backup file is very small ({backup_size} bytes)", Colors.YELLOW)
                self.log("This may indicate an incomplete backup", Colors.YELLOW)
                return False, backup_path
            
            # Success
            self.log(f"\n{Colors.GREEN}{Colors.BOLD}✓ Backup created successfully!{Colors.RESET}")
            self.log(f"File: {backup_path}")
            self.log(f"Size: {backup_size:,} bytes ({backup_size / (1024*1024):.2f} MB)")
            
            return True, backup_path
        
        except Exception as e:
            self.log(f"ERROR: Backup failed: {e}", Colors.RED)
            return False, None
    
    def cleanup_old_backups(self, backup_dir: str):
        """Remove old backups based on retention policy"""
        if not self.retention_days:
            return
        
        self.log(f"\n{Colors.BOLD}Cleaning up old backups...{Colors.RESET}")
        self.log(f"Retention policy: {self.retention_days} days")
        
        # Find all backup files
        patterns = [
            'slurm_acct_db_backup_*.sql',
            'slurm_acct_db_backup_*.sql.gz',
            'slurm_backup_*.sql',
            'slurm_backup_*.sql.gz',
        ]
        
        cutoff_time = datetime.now() - timedelta(days=self.retention_days)
        removed_count = 0
        
        for pattern in patterns:
            for filepath in glob.glob(os.path.join(backup_dir, pattern)):
                if os.path.isfile(filepath):
                    mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                    
                    if mtime < cutoff_time:
                        try:
                            os.remove(filepath)
                            self.log_verbose(f"Removed old backup: {os.path.basename(filepath)}")
                            removed_count += 1
                        except Exception as e:
                            self.log(f"Warning: Could not remove {filepath}: {e}", Colors.YELLOW)
        
        if removed_count > 0:
            self.log(f"{Colors.GREEN}✓ Removed {removed_count} old backup(s){Colors.RESET}")
        else:
            self.log("No old backups to remove")
    
    def verify_backup(self, backup_path: str) -> bool:
        """Verify the backup file is valid"""
        self.log(f"\n{Colors.BOLD}Verifying backup...{Colors.RESET}")
        
        if not os.path.exists(backup_path):
            self.log("ERROR: Backup file does not exist", Colors.RED)
            return False
        
        # Check if compressed
        if backup_path.endswith('.gz'):
            # Test gzip integrity
            result = subprocess.run(
                ['gzip', '-t', backup_path],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                self.log(f"ERROR: Backup file is corrupted: {result.stderr}", Colors.RED)
                return False
            
            # Check contents
            result = subprocess.run(
                ['bash', '-c', f'zcat "{backup_path}" | head -n 20'],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                content = result.stdout
            else:
                self.log("Warning: Could not read backup contents", Colors.YELLOW)
                return True  # At least gzip is valid
        else:
            # Read plain SQL file
            try:
                with open(backup_path, 'r') as f:
                    content = ''.join([f.readline() for _ in range(20)])
            except Exception as e:
                self.log(f"ERROR: Could not read backup: {e}", Colors.RED)
                return False
        
        # Check for Slurm database markers
        checks = {
            'SQL dump format': any(marker in content for marker in ['MySQL dump', 'MariaDB dump', 'CREATE TABLE']),
            'Slurm database': 'slurm' in content.lower() and 'acct' in content.lower(),
            'Slurm tables': any(table in content for table in ['acct_coord_table', 'cluster_table', 'job_table', 'qos_table']),
        }
        
        all_passed = True
        for check_name, passed in checks.items():
            if passed:
                self.log(f"  {Colors.GREEN}✓{Colors.RESET} {check_name}")
            else:
                self.log(f"  {Colors.RED}✗{Colors.RESET} {check_name}")
                all_passed = False
        
        if all_passed:
            self.log(f"\n{Colors.GREEN}{Colors.BOLD}✓ Backup verification passed!{Colors.RESET}")
        else:
            self.log(f"\n{Colors.YELLOW}⚠ Backup verification had warnings{Colors.RESET}")
        
        return all_passed
    
    def _discover_slurmdbd_nodes(self) -> list:
        """Discover nodes that run slurmdbd via BCM slurmaccounting role.
        
        Uses 'device; foreach -l slurmaccounting' to find devices that have
        the slurmaccounting role assigned (directly or via overlay).
        
        Returns:
            List of node hostnames that run slurmdbd
        """
        nodes = []
        cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
        
        if not os.path.exists(cmsh_path):
            self.log("  cmsh not found, cannot discover slurmdbd nodes")
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
                self.log(f"  Could not query devices with slurmaccounting role: {result.stderr}")
                return nodes
            
            for line in result.stdout.strip().split('\n'):
                line = line.strip()
                if line:
                    nodes.append(line)
            
            if nodes:
                self.log(f"  Found slurmdbd nodes: {', '.join(nodes)}")
            else:
                self.log(f"  {Colors.YELLOW}⚠{Colors.RESET} No devices found with slurmaccounting role")
            
        except Exception as e:
            self.log(f"  Warning: Could not discover slurmdbd nodes via cmsh: {e}", Colors.YELLOW)
        
        return nodes
    
    def _stop_slurmdbd_via_cmsh(self) -> bool:
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
            self.log(f"  Error running cmsh stop command: {e}", Colors.YELLOW)
            return False
    
    def _prepare_for_restore(self) -> bool:
        """Prepare the database for restore by stopping slurmdbd and killing connections.
        
        Returns:
            True if ready to proceed, False if preparation failed
        """
        self.log(f"\n{Colors.BOLD}Preparing database for restore...{Colors.RESET}")
        
        storage_host = self.db_config['storage_host']
        storage_user = self.db_config['storage_user']
        storage_pass = self.db_config['storage_pass']
        storage_loc = self.db_config['storage_loc']
        
        cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
        
        # Discover nodes that run slurmdbd
        self.log(f"  Discovering slurmdbd nodes via BCM...")
        slurmdbd_nodes = self._discover_slurmdbd_nodes()
        
        if not slurmdbd_nodes:
            self.log(f"  {Colors.YELLOW}⚠{Colors.RESET} Could not discover slurmdbd nodes from BCM")
            self.log(f"    You may need to manually stop slurmdbd before proceeding")
        
        # Check which nodes have slurmdbd running
        nodes_with_slurmdbd = []
        if slurmdbd_nodes:
            self.log(f"\n  Checking slurmdbd status...")
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
                        self.log(f"    {node}: slurmdbd is {Colors.YELLOW}running{Colors.RESET}")
                    else:
                        self.log(f"    {node}: slurmdbd is stopped")
                except Exception as e:
                    self.log(f"    {node}: could not check ({e})")
        
        # Stop slurmdbd if running - use cmsh to prevent BCM auto-restart
        if nodes_with_slurmdbd:
            self.log(f"\n  {Colors.YELLOW}Found slurmdbd running on: {', '.join(nodes_with_slurmdbd)}{Colors.RESET}")
            answer = input(f"  Stop slurmdbd via cmsh (prevents BCM auto-restart)? [Y/n]: ").strip().lower()
            if answer not in ('n', 'no'):
                self.log(f"    Stopping slurmdbd via cmsh...")
                if self._stop_slurmdbd_via_cmsh():
                    self.log(f"    {Colors.GREEN}✓{Colors.RESET} Stopped slurmdbd on all slurmaccounting nodes")
                else:
                    self.log(f"    {Colors.YELLOW}⚠{Colors.RESET} cmsh stop command may have failed")
                # Give services time to fully stop
                time.sleep(2)
            else:
                self.log(f"\n  {Colors.YELLOW}⚠ Warning: slurmdbd still running - restore may fail or corrupt data{Colors.RESET}")
        else:
            self.log(f"\n  {Colors.GREEN}✓{Colors.RESET} No slurmdbd services found running")
        
        # Check for blocking database connections
        self.log(f"\n  Checking for blocking database connections...")
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
            self.log(f"  Warning: Could not check for blocking connections: {e}", Colors.YELLOW)
        
        # Kill blocking connections
        if blocking_connections:
            self.log(f"  Found {len(blocking_connections)} connection(s) that may block restore:")
            for conn in blocking_connections:
                self.log(f"    - ID {conn['id']}: {conn['user']}@{conn['host']} (idle {conn['time']}s)")
            
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
                            self.log(f"    {Colors.GREEN}✓{Colors.RESET} Killed connection {conn['id']}")
                        else:
                            # Connection may have already closed
                            self.log(f"    {Colors.YELLOW}⚠{Colors.RESET} Connection {conn['id']} already closed or could not kill")
                    except Exception as e:
                        self.log(f"    {Colors.YELLOW}⚠{Colors.RESET} Error killing connection {conn['id']}: {e}")
                # Give a moment for connections to fully close
                time.sleep(1)
            else:
                self.log(f"\n{Colors.YELLOW}Warning: Restore may hang waiting for locks.{Colors.RESET}")
        else:
            self.log(f"  {Colors.GREEN}✓{Colors.RESET} No blocking connections found")
        
        self.log(f"\n  {Colors.GREEN}✓{Colors.RESET} Database ready for restore")
        return True
    
    def restore_backup(self, backup_file: str, force: bool = False) -> bool:
        """Restore database from a backup file.
        
        Reads StorageHost from slurmdbd.conf and restores to that database server.
        Supports both plain .sql and compressed .sql.gz files.
        
        Args:
            backup_file: Path to the backup file
            force: If True, skip confirmation prompt
            
        Returns:
            True if restore was successful, False otherwise
        """
        # Find and parse slurmdbd.conf
        self.slurmdbd_conf_path = self.find_slurmdbd_conf()
        
        if not self.slurmdbd_conf_path:
            self.log("ERROR: Could not find slurmdbd.conf", Colors.RED)
            self.log("Checked common locations in /cm/shared, /cm/local, /etc/slurm", Colors.RED)
            return False
        
        self.log(f"Reading database configuration from: {self.slurmdbd_conf_path}")
        
        self.db_config = self.parse_slurmdbd_conf(self.slurmdbd_conf_path)
        
        if not self.db_config:
            self.log("ERROR: Failed to parse slurmdbd.conf", Colors.RED)
            return False
        
        # Validate backup file exists
        if not os.path.exists(backup_file):
            self.log(f"ERROR: Backup file not found: {backup_file}", Colors.RED)
            return False
        
        backup_size = os.path.getsize(backup_file)
        is_compressed = backup_file.endswith('.gz')
        
        # Display restore information
        self.log(f"\n{Colors.BOLD}Restore Configuration:{Colors.RESET}")
        self.log(f"  Backup file     : {backup_file}")
        self.log(f"  File size       : {backup_size:,} bytes ({backup_size / (1024*1024):.2f} MB)")
        self.log(f"  Compressed      : {'Yes (.gz)' if is_compressed else 'No (plain SQL)'}")
        self.log(f"  Target host     : {self.db_config['storage_host']}")
        self.log(f"  Target database : {self.db_config['storage_loc']}")
        self.log(f"  Database user   : {self.db_config['storage_user']}")
        self.log(f"  Config source   : {self.slurmdbd_conf_path}")
        
        # Verify backup first
        self.log(f"\n{Colors.BOLD}Pre-restore verification...{Colors.RESET}")
        
        if is_compressed:
            # Test gzip integrity
            result = subprocess.run(
                ['gzip', '-t', backup_file],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                self.log(f"ERROR: Backup file is corrupted: {result.stderr}", Colors.RED)
                return False
            
            self.log(f"  {Colors.GREEN}✓{Colors.RESET} Gzip integrity check passed")
        
        # Confirmation prompt
        if not force:
            self.log(f"\n{Colors.YELLOW}{Colors.BOLD}WARNING:{Colors.RESET}")
            self.log(f"{Colors.YELLOW}This will REPLACE the contents of database '{self.db_config['storage_loc']}'")
            self.log(f"on host '{self.db_config['storage_host']}' with the backup data.{Colors.RESET}")
            self.log(f"{Colors.YELLOW}Any existing data in that database will be LOST.{Colors.RESET}\n")
            
            answer = input("Are you sure you want to proceed? [y/N]: ").strip().lower()
            if answer not in ('y', 'yes'):
                self.log("\nRestore cancelled by user.")
                return False
        
        # Pre-restore: Stop slurmdbd and kill blocking connections
        if not self._prepare_for_restore():
            return False
        
        # Build mysql command
        mysql_cmd = [
            'mysql',
            f"--host={self.db_config['storage_host']}",
            f"--port={self.db_config['storage_port']}",
            f"--user={self.db_config['storage_user']}",
            '--default-character-set=utf8mb4',
        ]
        
        if self.db_config['storage_pass']:
            mysql_cmd.append(f"--password={self.db_config['storage_pass']}")
        
        # Create database if it doesn't exist
        self.log(f"\n{Colors.BOLD}Ensuring database exists...{Colors.RESET}")
        
        create_db_cmd = mysql_cmd + [
            '-e',
            f"CREATE DATABASE IF NOT EXISTS `{self.db_config['storage_loc']}` "
            f"DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        ]
        
        result = subprocess.run(
            create_db_cmd,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            # Filter out password warning
            stderr = '\n'.join([l for l in result.stderr.split('\n') 
                               if 'password on the command line' not in l.lower()])
            if stderr.strip():
                self.log(f"ERROR: Could not create database: {stderr}", Colors.RED)
                return False
        
        self.log(f"  {Colors.GREEN}✓{Colors.RESET} Database exists or created")
        
        # Perform restore with progress feedback
        self.log(f"\n{Colors.BOLD}Restoring database...{Colors.RESET}")
        self.log(f"  Source file: {backup_size:,} bytes ({backup_size / 1024 / 1024:.1f} MB)")
        self.log("")
        
        restore_cmd = mysql_cmd + [self.db_config['storage_loc']]
        
        try:
            start_time = time.time()
            
            # Progress tracking with elapsed time spinner and DB status
            restore_complete = [False]
            last_table_count = [0]
            
            def get_table_count():
                """Query database for current table count"""
                try:
                    check_cmd = [
                        'mysql',
                        '-h', self.db_config['storage_host'],
                        '-u', self.db_config['storage_user'],
                        f"-p{self.db_config['storage_pass']}",
                        '-N', '-e',
                        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='{self.db_config['storage_loc']}';"
                    ]
                    result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=5)
                    if result.returncode == 0:
                        return int(result.stdout.strip())
                except:
                    pass
                return 0
            
            def show_elapsed():
                """Show elapsed time while restore runs"""
                spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
                spin_idx = 0
                check_interval = 50  # Check DB every 5 seconds (50 * 0.1s)
                check_counter = 0
                
                while not restore_complete[0]:
                    elapsed = time.time() - start_time
                    mins = int(elapsed // 60)
                    secs = int(elapsed % 60)
                    
                    # Periodically check table count to verify progress
                    check_counter += 1
                    if check_counter >= check_interval:
                        check_counter = 0
                        count = get_table_count()
                        if count > 0:
                            last_table_count[0] = count
                    
                    # Show status with table count if available
                    if last_table_count[0] > 0:
                        status = f"\r  {spinner[spin_idx]} Importing... {mins:02d}:{secs:02d} elapsed | {last_table_count[0]} tables   "
                    else:
                        status = f"\r  {spinner[spin_idx]} Importing... {mins:02d}:{secs:02d} elapsed   "
                    
                    sys.stdout.write(status)
                    sys.stdout.flush()
                    spin_idx = (spin_idx + 1) % len(spinner)
                    time.sleep(0.1)
                sys.stdout.write("\n")
                sys.stdout.flush()
            
            # Start elapsed time display thread
            elapsed_thread = threading.Thread(target=show_elapsed, daemon=True)
            elapsed_thread.start()
            
            if is_compressed:
                # Decompress and pipe to mysql
                zcat_process = subprocess.Popen(
                    ['zcat', backup_file],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                
                mysql_process = subprocess.Popen(
                    restore_cmd,
                    stdin=zcat_process.stdout,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                zcat_process.stdout.close()
                mysql_stdout, mysql_stderr = mysql_process.communicate()
                zcat_process.wait()
                
                restore_complete[0] = True
                elapsed_thread.join(timeout=2)
                
                if zcat_process.returncode != 0:
                    self.log("ERROR: Failed to decompress backup file", Colors.RED)
                    return False
                
                if mysql_process.returncode != 0:
                    stderr = '\n'.join([l for l in mysql_stderr.split('\n') 
                                       if 'password on the command line' not in l.lower()])
                    if stderr.strip():
                        self.log(f"ERROR: MySQL restore failed: {stderr}", Colors.RED)
                        return False
            else:
                # Plain SQL file
                mysql_process = subprocess.Popen(
                    restore_cmd,
                    stdin=open(backup_file, 'r'),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                mysql_stdout, mysql_stderr = mysql_process.communicate()
                
                restore_complete[0] = True
                elapsed_thread.join(timeout=2)
                
                if mysql_process.returncode != 0:
                    stderr = '\n'.join([l for l in mysql_stderr.split('\n') 
                                       if 'password on the command line' not in l.lower()])
                    if stderr.strip():
                        self.log(f"ERROR: MySQL restore failed: {stderr}", Colors.RED)
                        return False
            
            # Show completion stats
            total_time = time.time() - start_time
            mins = int(total_time // 60)
            secs = int(total_time % 60)
            self.log(f"\n  Completed in {mins:02d}:{secs:02d}")
            
            self.log(f"\n{Colors.GREEN}{Colors.BOLD}✓ Database restored successfully!{Colors.RESET}")
            self.log(f"  Database: {self.db_config['storage_loc']}")
            self.log(f"  Host: {self.db_config['storage_host']}")
            
            # Post-restore advice
            self.log(f"\n{Colors.BOLD}Post-restore steps:{Colors.RESET}")
            self.log("  1. Restart slurmdbd service: systemctl restart slurmdbd")
            self.log("  2. Verify with: sacctmgr show cluster")
            self.log("  3. Check accounting: sacctmgr show account")
            
            return True
        
        except Exception as e:
            self.log(f"ERROR: Restore failed: {e}", Colors.RED)
            return False


def main():
    parser = argparse.ArgumentParser(
        description='Backup and restore Slurm accounting database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backup operations
  %(prog)s                          # Create compressed backup in default location
  %(prog)s -o /backup/slurm         # Backup to specific directory
  %(prog)s --retention 7            # Keep only last 7 days of backups
  %(prog)s --no-compress            # Create uncompressed backup
  %(prog)s -v                       # Verbose output
  %(prog)s --verify-only FILE       # Just verify an existing backup
  
  # Restore operations
  %(prog)s --restore backup.sql.gz  # Restore from backup file (with confirmation)
  %(prog)s --restore backup.sql -y  # Restore without confirmation prompt

Notes:
  - Both backup and restore use the StorageHost from slurmdbd.conf
  - No need to specify which server - it's determined from the config
  - Supports both .sql and .sql.gz backup files
        """
    )
    
    # Backup arguments
    parser.add_argument('-o', '--output-dir', type=str,
                        help='Output directory for backup (default: auto-detect)')
    parser.add_argument('--no-compress', action='store_true',
                        help='Do not compress backup with gzip')
    parser.add_argument('--retention', type=int, metavar='DAYS',
                        help='Remove backups older than N days')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose output')
    parser.add_argument('--verify-only', type=str, metavar='FILE',
                        help='Only verify an existing backup file')
    
    # Restore arguments
    parser.add_argument('--restore', type=str, metavar='FILE',
                        help='Restore database from backup file')
    parser.add_argument('-y', '--yes', action='store_true',
                        help='Skip confirmation prompt (use with --restore)')
    
    args = parser.parse_args()
    
    # Check if running as root
    if os.geteuid() != 0:
        print(f"{Colors.YELLOW}Warning: Not running as root. You may encounter permission issues.{Colors.RESET}")
    
    backup = SlurmDatabaseBackup(
        output_dir=args.output_dir,
        compress=not args.no_compress,
        retention_days=args.retention,
        verbose=args.verbose
    )
    
    # Verify-only mode
    if args.verify_only:
        if not os.path.exists(args.verify_only):
            print(f"{Colors.RED}ERROR: File not found: {args.verify_only}{Colors.RESET}")
            sys.exit(1)
        
        success = backup.verify_backup(args.verify_only)
        sys.exit(0 if success else 1)
    
    # Restore mode
    if args.restore:
        if not os.path.exists(args.restore):
            print(f"{Colors.RED}ERROR: Backup file not found: {args.restore}{Colors.RESET}")
            sys.exit(1)
        
        print(f"{Colors.BOLD}{'=' * 65}")
        print("SLURM DATABASE RESTORE")
        print('=' * 65 + Colors.RESET)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        success = backup.restore_backup(args.restore, force=args.yes)
        
        if success:
            print(f"\n{Colors.BOLD}{'=' * 65}{Colors.RESET}")
            sys.exit(0)
        else:
            print(f"\n{Colors.RED}{Colors.BOLD}✗ Restore failed!{Colors.RESET}")
            sys.exit(1)
    
    # Create backup
    print(f"{Colors.BOLD}{'=' * 65}")
    print("SLURM DATABASE BACKUP")
    print('=' * 65 + Colors.RESET)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    success, backup_path = backup.create_backup()
    
    if not success:
        print(f"\n{Colors.RED}{Colors.BOLD}✗ Backup failed!{Colors.RESET}")
        sys.exit(1)
    
    # Verify backup
    if backup_path:
        backup.verify_backup(backup_path)
    
    # Cleanup old backups
    if args.retention and backup_path:
        backup.cleanup_old_backups(os.path.dirname(backup_path))
    
    print(f"\n{Colors.BOLD}{'=' * 65}{Colors.RESET}")
    sys.exit(0)


if __name__ == '__main__':
    main()

