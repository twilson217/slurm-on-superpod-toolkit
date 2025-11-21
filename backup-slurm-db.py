#!/usr/bin/env python3
"""
Slurm Database Backup Script

Creates a compressed backup of the Slurm accounting database (slurmdbd).
Reads database credentials from slurmdbd.conf and creates timestamped backups.

Usage:
    ./backup-slurm-db.py                    # Create backup in default directory
    ./backup-slurm-db.py -o /path/to/dir    # Create backup in specific directory
    ./backup-slurm-db.py --retention 7      # Keep only last 7 days of backups
    ./backup-slurm-db.py --no-compress      # Skip gzip compression
"""

import argparse
import subprocess
import sys
import os
import re
import configparser
from datetime import datetime, timedelta
from pathlib import Path
import glob


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


def main():
    parser = argparse.ArgumentParser(
        description='Backup Slurm accounting database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Create compressed backup in default location
  %(prog)s -o /backup/slurm         # Backup to specific directory
  %(prog)s --retention 7            # Keep only last 7 days of backups
  %(prog)s --no-compress            # Create uncompressed backup
  %(prog)s -v                       # Verbose output
  %(prog)s --verify-only FILE       # Just verify an existing backup
        """
    )
    
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

