#!/usr/bin/env python3
"""
Backup Slurm-related files from a BCM-managed cluster.

This script backs up:
  - Systemd unit files (slurm*, munge*, mysql*, mariadb*) from all Slurm nodes
  - Custom prolog/epilog scripts discovered from cmsh WLM settings
  - Prolog/epilog symlink targets in /cm/local/apps/slurm/var/{prologs,epilogs}/
  - Lua plugin files when enabled in slurm.conf (cli_filter.lua, job_submit.lua)
  - Config files in the Slurm etc directory (topology.conf, plugstack.conf.d/*, etc.)

The backup includes a manifest.json that tracks symlinks and their targets,
enabling accurate restoration with --restore.

Assumptions:
  - Run on a BCM head node as root
  - Passwordless SSH to all Slurm nodes
  - cmsh available (typically /cm/local/apps/cmd/bin/cmsh)

Usage:
  ./backup-slurm-files.py                    # Backup all
  ./backup-slurm-files.py --restore <dir>    # Restore from backup
  ./backup-slurm-files.py -o /path/to/backup # Custom output dir
  ./backup-slurm-files.py -v                 # Verbose
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple, Any


class Colors:
    """ANSI colors for human-friendly output."""

    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    @classmethod
    def disable(cls):
        cls.GREEN = cls.YELLOW = cls.RED = cls.BLUE = cls.BOLD = cls.RESET = ""


# Default BCM scripts that should NOT be backed up (they are part of BCM)
DEFAULT_BCM_SCRIPTS = {
    "/cm/local/apps/cmd/scripts/prolog",
    "/cm/local/apps/cmd/scripts/epilog",
}


class SlurmFilesBackup:
    """Main backup orchestrator for Slurm files."""

    def __init__(self, output_dir: str, verbose: bool = False):
        self.verbose = verbose
        self.cmsh_path: Optional[str] = None
        self.output_root = Path(output_dir).resolve()
        self.nodes_by_role: Dict[str, Set[str]] = {
            "slurmserver": set(),
            "slurmaccounting": set(),
            "slurmclient": set(),
            "slurmsubmit": set(),
        }
        # Manifest to track all backed up files
        self.manifest: Dict[str, Any] = {
            "created": datetime.now().isoformat(),
            "files": [],
        }
        # WLM settings from cmsh
        self.wlm_settings: Dict[str, str] = {}
        # Slurm paths discovered from cmsh
        self.slurm_prefix: str = "/cm/shared/apps/slurm"
        self.slurm_etc: str = "/cm/shared/apps/slurm/var/etc/slurm"

        if not sys.stdout.isatty():
            Colors.disable()

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def log(self, msg: str, color: str = ""):
        print(f"{color}{msg}{Colors.RESET}")

    def vlog(self, msg: str):
        if self.verbose:
            print(f"  {msg}")

    def run_local(self, cmd: List[str], timeout: int = 30) -> Tuple[int, str, str]:
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return -1, "", f"Command timed out after {timeout}s: {' '.join(cmd)}"
        except Exception as e:
            return -1, "", str(e)

    def run_ssh(self, node: str, cmd: str, timeout: int = 30) -> Tuple[int, str, str]:
        """Run a shell command via ssh on a remote node."""
        ssh_cmd = [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "ConnectTimeout=5",
            node,
            cmd,
        ]
        return self.run_local(ssh_cmd, timeout=timeout)

    # -------------------------------------------------------------------------
    # Discovery
    # -------------------------------------------------------------------------

    def find_cmsh(self) -> Optional[str]:
        """Locate the cmsh binary."""
        candidates = [
            "/cm/local/apps/cmd/bin/cmsh",
            "/usr/bin/cmsh",
            "/usr/local/bin/cmsh",
        ]
        for path in candidates:
            if os.path.exists(path) and os.access(path, os.X_OK):
                self.vlog(f"Found cmsh at {path}")
                return path
        return None

    def discover_nodes_by_role(self) -> List[str]:
        """Use cmsh to discover nodes that have various Slurm roles."""
        self.cmsh_path = self.find_cmsh()
        if not self.cmsh_path:
            self.log(
                "ERROR: cmsh binary not found. Ensure BCM is installed and cmsh is available.",
                Colors.RED,
            )
            sys.exit(1)

        for role in self.nodes_by_role.keys():
            cmd = f'{self.cmsh_path} -c "device; foreach -l {role} (get hostname)"'
            rc, out, err = self.run_local(["bash", "-c", cmd], timeout=20)
            if rc != 0:
                self.log(f"Warning: failed to list devices for role {role}: {err}", Colors.YELLOW)
                continue

            for line in out.splitlines():
                line = line.strip()
                if line:
                    self.nodes_by_role[role].add(line)

        all_nodes = set()
        for role, nodes in self.nodes_by_role.items():
            if nodes:
                self.vlog(f"Discovered {len(nodes)} node(s) with role {role}: {', '.join(sorted(nodes))}")
            all_nodes |= nodes

        if not all_nodes:
            self.log("ERROR: No Slurm-related nodes discovered via cmsh.", Colors.RED)
            sys.exit(1)

        self.log(
            f"Discovered {len(all_nodes)} unique node(s) with Slurm roles: "
            + ", ".join(sorted(all_nodes)),
            Colors.GREEN,
        )
        return sorted(all_nodes)

    def discover_wlm_settings(self):
        """Parse cmsh 'wlm; use slurm; show' to get WLM settings."""
        if not self.cmsh_path:
            self.cmsh_path = self.find_cmsh()
        if not self.cmsh_path:
            return

        cmd = f'{self.cmsh_path} -c "wlm; use slurm; show"'
        rc, out, err = self.run_local(["bash", "-c", cmd], timeout=20)
        if rc != 0:
            self.log(f"Warning: failed to get WLM settings: {err}", Colors.YELLOW)
            return

        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            # Format: "Setting Name                     value"
            # Split on first occurrence of multiple spaces
            parts = line.split(None, 1)
            if len(parts) >= 2:
                # Handle multi-word keys like "Prolog Slurmctld"
                # The key ends where there are 2+ spaces before the value
                import re
                match = re.match(r'^(.+?)\s{2,}(.*)$', line)
                if match:
                    key = match.group(1).strip()
                    value = match.group(2).strip()
                    self.wlm_settings[key] = value

        # Extract key paths
        if "Prefix" in self.wlm_settings:
            self.slurm_prefix = self.wlm_settings["Prefix"]
        if "Etc" in self.wlm_settings:
            self.slurm_etc = self.wlm_settings["Etc"]

        self.vlog(f"Slurm prefix: {self.slurm_prefix}")
        self.vlog(f"Slurm etc: {self.slurm_etc}")

    # -------------------------------------------------------------------------
    # Prolog/Epilog Script Discovery
    # -------------------------------------------------------------------------

    def get_custom_prolog_epilog_scripts(self) -> List[str]:
        """Get non-default prolog/epilog scripts from WLM settings."""
        scripts = []
        
        # Map of WLM setting names to check
        script_settings = [
            "Prolog Slurmctld",
            "Epilog Slurmctld",
            "Prolog",
            "Epilog",
            "Task Prolog",
            "Task Epilog",
            "Srun Prolog",
            "Srun Epilog",
        ]

        for setting in script_settings:
            value = self.wlm_settings.get(setting, "").strip()
            if value and value not in DEFAULT_BCM_SCRIPTS:
                scripts.append(value)
                self.vlog(f"Found custom {setting}: {value}")

        return scripts

    def get_prolog_epilog_symlinks(self) -> List[Dict[str, str]]:
        """Scan /cm/local/apps/slurm/var/{prologs,epilogs}/ for symlinks."""
        symlink_info = []
        
        dirs_to_scan = [
            "/cm/local/apps/slurm/var/prologs",
            "/cm/local/apps/slurm/var/epilogs",
        ]

        for scan_dir in dirs_to_scan:
            if not os.path.isdir(scan_dir):
                self.vlog(f"Directory not found: {scan_dir}")
                continue

            for entry in os.listdir(scan_dir):
                entry_path = os.path.join(scan_dir, entry)
                if os.path.islink(entry_path):
                    target = os.readlink(entry_path)
                    # Resolve to absolute path if relative
                    if not os.path.isabs(target):
                        target = os.path.normpath(os.path.join(scan_dir, target))
                    symlink_info.append({
                        "symlink_path": entry_path,
                        "target": target,
                    })
                    self.vlog(f"Found symlink: {entry_path} -> {target}")

        return symlink_info

    # -------------------------------------------------------------------------
    # Config File Discovery
    # -------------------------------------------------------------------------

    def get_slurm_config_files(self) -> List[Dict[str, Any]]:
        """Get all config files from the Slurm etc directory."""
        config_files = []
        
        if not os.path.isdir(self.slurm_etc):
            self.vlog(f"Slurm etc directory not found: {self.slurm_etc}")
            return config_files

        for entry in os.listdir(self.slurm_etc):
            entry_path = os.path.join(self.slurm_etc, entry)
            
            if os.path.islink(entry_path):
                target = os.readlink(entry_path)
                if not os.path.isabs(target):
                    target = os.path.normpath(os.path.join(self.slurm_etc, target))
                config_files.append({
                    "path": entry_path,
                    "type": "symlink",
                    "target": target,
                })
                self.vlog(f"Found config symlink: {entry_path} -> {target}")
            elif os.path.isfile(entry_path):
                config_files.append({
                    "path": entry_path,
                    "type": "file",
                })
                self.vlog(f"Found config file: {entry_path}")
            elif os.path.isdir(entry_path):
                # Recurse into directories like plugstack.conf.d/
                for sub_entry in os.listdir(entry_path):
                    sub_path = os.path.join(entry_path, sub_entry)
                    if os.path.isfile(sub_path):
                        config_files.append({
                            "path": sub_path,
                            "type": "file",
                        })
                        self.vlog(f"Found config file: {sub_path}")

        return config_files

    def get_lua_plugin_files(self) -> List[Dict[str, Any]]:
        """Check slurm.conf for Lua plugins and find their files."""
        lua_files = []
        slurm_conf = os.path.join(self.slurm_etc, "slurm.conf")
        
        if not os.path.isfile(slurm_conf):
            self.vlog(f"slurm.conf not found at {slurm_conf}")
            return lua_files

        # Lua plugin mapping: config option -> expected lua file
        lua_plugins = {
            "CliFilterPlugins": "cli_filter.lua",
            "JobSubmitPlugins": "job_submit.lua",
        }

        try:
            with open(slurm_conf, 'r') as f:
                content = f.read()
        except Exception as e:
            self.vlog(f"Cannot read slurm.conf: {e}")
            return lua_files

        for plugin_key, lua_file in lua_plugins.items():
            # Look for uncommented lines containing the plugin
            for line in content.splitlines():
                line = line.strip()
                if line.startswith('#'):
                    continue
                if plugin_key in line and 'lua' in line.lower():
                    lua_path = os.path.join(self.slurm_etc, lua_file)
                    if os.path.islink(lua_path):
                        target = os.readlink(lua_path)
                        if not os.path.isabs(target):
                            target = os.path.normpath(os.path.join(self.slurm_etc, target))
                        lua_files.append({
                            "path": lua_path,
                            "type": "symlink",
                            "target": target,
                        })
                        self.vlog(f"Found Lua plugin symlink: {lua_path} -> {target}")
                    elif os.path.isfile(lua_path):
                        lua_files.append({
                            "path": lua_path,
                            "type": "file",
                        })
                        self.vlog(f"Found Lua plugin file: {lua_path}")
                    break

        return lua_files

    # -------------------------------------------------------------------------
    # Remote systemd inspection and backup
    # -------------------------------------------------------------------------

    def list_slurm_related_units(self, node: str) -> List[str]:
        """List systemd unit files on a node for Slurm and related services."""
        patterns = ["slurm*", "munge*", "mysql*", "mariadb*"]
        all_units = []
        
        for pattern in patterns:
            cmd = f"systemctl list-unit-files '{pattern}' --no-legend --no-pager 2>/dev/null"
            rc, out, err = self.run_ssh(node, cmd, timeout=20)
            if rc != 0:
                self.vlog(f"[{node}] Warning: unable to list {pattern} unit files: {err}")
                continue

            for line in out.splitlines():
                line = line.strip()
                if not line:
                    continue
                parts = line.split()
                unit = parts[0]
                if unit.endswith(".service"):
                    all_units.append(unit)

        # Deduplicate while preserving order
        seen = set()
        units = []
        for unit in all_units:
            if unit not in seen:
                seen.add(unit)
                units.append(unit)

        self.vlog(f"[{node}] Found units: {', '.join(units) if units else 'none'}")
        return units

    def find_remote_unit_paths(self, node: str, unit: str) -> List[str]:
        """Find the full paths of a unit file and any drop-in directories."""
        show_cmd = (
            f"systemctl show {unit} -p FragmentPath -p DropInPaths --no-pager 2>/dev/null"
        )
        rc, out, err = self.run_ssh(node, show_cmd, timeout=20)
        if rc != 0 or not out.strip():
            self.vlog(f"[{node}] Unable to query unit paths for {unit}: {err}")
            return []

        fragment_path: Optional[str] = None
        dropin_paths: List[str] = []

        for line in out.splitlines():
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip()
            if key == "FragmentPath" and val:
                fragment_path = val
            elif key == "DropInPaths" and val:
                dropin_paths.extend(p for p in val.split() if p)

        paths: List[str] = []
        if fragment_path:
            paths.append(fragment_path)
        paths.extend(dropin_paths)

        self.vlog(f"[{node}] Paths for {unit}: {', '.join(paths) if paths else 'none'}")
        return paths

    def backup_remote_path(self, node: str, remote_path: str) -> Optional[str]:
        """Backup a single remote path (file or directory) using scp."""
        rel_remote = remote_path.lstrip("/")
        local_dir = self.output_root / "systemd" / node / os.path.dirname(rel_remote)
        local_dir.mkdir(parents=True, exist_ok=True)
        local_file = local_dir / os.path.basename(remote_path)

        self.vlog(f"[{node}] Backing up {remote_path} -> {local_dir}")

        scp_cmd = [
            "scp",
            "-r",
            "-o",
            "StrictHostKeyChecking=no",
            f"{node}:{remote_path}",
            str(local_dir),
        ]

        rc, out, err = self.run_local(scp_cmd, timeout=60)
        if rc != 0:
            self.log(
                f"Warning: failed to copy {remote_path} from {node}: {err}",
                Colors.YELLOW,
            )
            return None
        else:
            self.vlog(f"[{node}] Copied {remote_path}")
            return str(local_file.relative_to(self.output_root))

    def backup_node_units(self, node: str):
        """Backup all Slurm-related systemd unit files from a node."""
        units = self.list_slurm_related_units(node)
        if not units:
            self.log(f"[{node}] No Slurm-related service units found (skipping)", Colors.YELLOW)
            return

        self.log(f"[{node}] Backing up {len(units)} systemd unit(s)...", Colors.BLUE)

        for unit in units:
            paths = self.find_remote_unit_paths(node, unit)
            if not paths:
                self.log(
                    f"[{node}] Warning: unit {unit} has no resolvable path (skipping)",
                    Colors.YELLOW,
                )
                continue

            for path in paths:
                backup_file = self.backup_remote_path(node, path)
                if backup_file:
                    self.manifest["files"].append({
                        "path": path,
                        "type": "file",
                        "node": node,
                        "category": "systemd",
                        "backup_file": backup_file,
                    })

    # -------------------------------------------------------------------------
    # Local file backup
    # -------------------------------------------------------------------------

    def backup_local_file(self, file_path: str, category: str = "config") -> Optional[str]:
        """Backup a local file, handling symlinks properly."""
        if not os.path.exists(file_path) and not os.path.islink(file_path):
            self.vlog(f"File not found: {file_path}")
            return None

        rel_path = file_path.lstrip("/")
        local_dest = self.output_root / category / rel_path
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        is_symlink = os.path.islink(file_path)
        symlink_target = None

        if is_symlink:
            symlink_target = os.readlink(file_path)
            if not os.path.isabs(symlink_target):
                symlink_target = os.path.normpath(os.path.join(os.path.dirname(file_path), symlink_target))
            
            # Backup the actual target file content
            if os.path.exists(symlink_target):
                try:
                    shutil.copy2(symlink_target, local_dest)
                    self.vlog(f"Backed up symlink target: {symlink_target} -> {local_dest}")
                except Exception as e:
                    self.log(f"Warning: failed to copy {symlink_target}: {e}", Colors.YELLOW)
                    return None
            else:
                self.log(f"Warning: symlink target does not exist: {symlink_target}", Colors.YELLOW)
                return None
        else:
            try:
                shutil.copy2(file_path, local_dest)
                self.vlog(f"Backed up file: {file_path} -> {local_dest}")
            except Exception as e:
                self.log(f"Warning: failed to copy {file_path}: {e}", Colors.YELLOW)
                return None

        backup_file = str(local_dest.relative_to(self.output_root))

        manifest_entry = {
            "path": file_path,
            "type": "symlink" if is_symlink else "file",
            "category": category,
            "backup_file": backup_file,
        }
        if symlink_target:
            manifest_entry["target"] = symlink_target

        self.manifest["files"].append(manifest_entry)
        return backup_file

    def backup_prolog_epilog_symlink(self, symlink_info: Dict[str, str]) -> Optional[str]:
        """Backup a prolog/epilog symlink and its target."""
        symlink_path = symlink_info["symlink_path"]
        target = symlink_info["target"]

        if not os.path.exists(target):
            self.log(f"Warning: symlink target does not exist: {target}", Colors.YELLOW)
            return None

        # Backup the target file
        rel_target = target.lstrip("/")
        local_dest = self.output_root / "scripts" / rel_target
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        try:
            shutil.copy2(target, local_dest)
            self.vlog(f"Backed up script: {target} -> {local_dest}")
        except Exception as e:
            self.log(f"Warning: failed to copy {target}: {e}", Colors.YELLOW)
            return None

        backup_file = str(local_dest.relative_to(self.output_root))

        self.manifest["files"].append({
            "path": symlink_path,
            "type": "symlink",
            "target": target,
            "category": "prolog_epilog",
            "backup_file": backup_file,
        })

        return backup_file

    # -------------------------------------------------------------------------
    # Manifest
    # -------------------------------------------------------------------------

    def save_manifest(self):
        """Save the manifest.json file."""
        manifest_path = self.output_root / "manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(self.manifest, f, indent=2)
        self.vlog(f"Saved manifest to {manifest_path}")

    # -------------------------------------------------------------------------
    # Orchestration
    # -------------------------------------------------------------------------

    def run_backup(self):
        """Run the full backup process."""
        self.log(
            f"{Colors.BOLD}Backing up Slurm files to: {self.output_root}{Colors.RESET}"
        )
        self.output_root.mkdir(parents=True, exist_ok=True)

        # Discover WLM settings first
        self.log("\n[1/5] Discovering WLM settings...", Colors.BLUE)
        self.discover_wlm_settings()

        # Discover nodes
        self.log("\n[2/5] Discovering Slurm nodes...", Colors.BLUE)
        nodes = self.discover_nodes_by_role()

        # Backup systemd unit files from all nodes
        self.log("\n[3/5] Backing up systemd unit files...", Colors.BLUE)
        for node in nodes:
            self.backup_node_units(node)

        # Backup custom prolog/epilog scripts
        self.log("\n[4/5] Backing up prolog/epilog scripts...", Colors.BLUE)
        
        # Get non-default scripts from WLM settings
        custom_scripts = self.get_custom_prolog_epilog_scripts()
        for script_path in custom_scripts:
            if os.path.exists(script_path):
                self.backup_local_file(script_path, category="scripts")
            else:
                self.log(f"Warning: custom script not found: {script_path}", Colors.YELLOW)

        # Get symlinks from prolog/epilog directories
        symlinks = self.get_prolog_epilog_symlinks()
        for symlink_info in symlinks:
            self.backup_prolog_epilog_symlink(symlink_info)

        # Backup config files
        self.log("\n[5/5] Backing up config files...", Colors.BLUE)
        
        # Get all config files
        config_files = self.get_slurm_config_files()
        for config_info in config_files:
            self.backup_local_file(config_info["path"], category="config")

        # Get Lua plugin files
        lua_files = self.get_lua_plugin_files()
        for lua_info in lua_files:
            # Skip if already backed up in config files
            already_backed_up = any(
                f["path"] == lua_info["path"] for f in self.manifest["files"]
            )
            if not already_backed_up:
                self.backup_local_file(lua_info["path"], category="config")

        # Save manifest
        self.save_manifest()

        # Summary
        file_count = len(self.manifest["files"])
        self.log(f"\n{Colors.GREEN}{Colors.BOLD}Backup completed!{Colors.RESET}")
        self.log(f"  Total files backed up: {file_count}")
        self.log(f"  Backup directory: {self.output_root}")
        self.log(f"  Manifest: {self.output_root}/manifest.json")


class SlurmFilesRestore:
    """Restore Slurm files from a backup."""

    def __init__(self, backup_dir: str, verbose: bool = False):
        self.verbose = verbose
        self.backup_root = Path(backup_dir).resolve()
        self.manifest: Dict[str, Any] = {}

        if not sys.stdout.isatty():
            Colors.disable()

    def log(self, msg: str, color: str = ""):
        print(f"{color}{msg}{Colors.RESET}")

    def vlog(self, msg: str):
        if self.verbose:
            print(f"  {msg}")

    def load_manifest(self) -> bool:
        """Load the manifest.json file."""
        manifest_path = self.backup_root / "manifest.json"
        if not manifest_path.exists():
            self.log(f"ERROR: manifest.json not found in {self.backup_root}", Colors.RED)
            return False

        try:
            with open(manifest_path, 'r') as f:
                self.manifest = json.load(f)
            self.log(f"Loaded manifest from {manifest_path}")
            return True
        except Exception as e:
            self.log(f"ERROR: Failed to load manifest: {e}", Colors.RED)
            return False

    def restore_file(self, entry: Dict[str, Any]) -> bool:
        """Restore a single file from the backup."""
        original_path = entry["path"]
        backup_file = entry.get("backup_file")
        file_type = entry.get("type", "file")
        target = entry.get("target")
        node = entry.get("node")

        # Skip remote node files (systemd units on other nodes)
        if node:
            self.vlog(f"Skipping remote file (node={node}): {original_path}")
            return True

        # Check if file already exists
        if os.path.exists(original_path) or os.path.islink(original_path):
            self.vlog(f"File exists, skipping: {original_path}")
            return True

        if not backup_file:
            self.log(f"Warning: no backup_file for {original_path}", Colors.YELLOW)
            return False

        backup_path = self.backup_root / backup_file
        if not backup_path.exists():
            self.log(f"Warning: backup file not found: {backup_path}", Colors.YELLOW)
            return False

        # Create parent directory if needed
        original_parent = os.path.dirname(original_path)
        if not os.path.exists(original_parent):
            try:
                os.makedirs(original_parent, exist_ok=True)
                self.vlog(f"Created directory: {original_parent}")
            except Exception as e:
                self.log(f"Error creating directory {original_parent}: {e}", Colors.RED)
                return False

        if file_type == "symlink" and target:
            # First, ensure the target file exists
            if not os.path.exists(target):
                # Try to restore the target file first
                target_parent = os.path.dirname(target)
                if not os.path.exists(target_parent):
                    try:
                        os.makedirs(target_parent, exist_ok=True)
                    except Exception as e:
                        self.log(f"Error creating directory {target_parent}: {e}", Colors.RED)
                        return False

                try:
                    shutil.copy2(backup_path, target)
                    self.log(f"Restored target file: {target}", Colors.GREEN)
                except Exception as e:
                    self.log(f"Error restoring target {target}: {e}", Colors.RED)
                    return False

            # Create the symlink
            try:
                os.symlink(target, original_path)
                self.log(f"Restored symlink: {original_path} -> {target}", Colors.GREEN)
            except Exception as e:
                self.log(f"Error creating symlink {original_path}: {e}", Colors.RED)
                return False
        else:
            # Regular file
            try:
                shutil.copy2(backup_path, original_path)
                self.log(f"Restored file: {original_path}", Colors.GREEN)
            except Exception as e:
                self.log(f"Error restoring {original_path}: {e}", Colors.RED)
                return False

        return True

    def run_restore(self):
        """Run the restore process."""
        if not self.load_manifest():
            sys.exit(1)

        files = self.manifest.get("files", [])
        if not files:
            self.log("No files in manifest to restore.", Colors.YELLOW)
            return

        self.log(f"\n{Colors.BOLD}Restoring files from: {self.backup_root}{Colors.RESET}")
        self.log(f"Manifest contains {len(files)} file(s)\n")

        restored = 0
        skipped = 0
        failed = 0

        for entry in files:
            original_path = entry["path"]
            node = entry.get("node")

            # Skip remote node files
            if node:
                skipped += 1
                continue

            if os.path.exists(original_path) or os.path.islink(original_path):
                skipped += 1
                self.vlog(f"Exists, skipping: {original_path}")
            else:
                if self.restore_file(entry):
                    restored += 1
                else:
                    failed += 1

        self.log(f"\n{Colors.BOLD}Restore Summary:{Colors.RESET}")
        self.log(f"  Restored: {restored}", Colors.GREEN if restored else "")
        self.log(f"  Skipped (exists): {skipped}")
        if failed:
            self.log(f"  Failed: {failed}", Colors.RED)


def main():
    parser = argparse.ArgumentParser(
        description="Backup and restore Slurm-related files from a BCM-managed cluster",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  backup-slurm-files.py                      # Backup all Slurm files
  backup-slurm-files.py -o ./my-backup       # Custom output directory
  backup-slurm-files.py --restore ./backup   # Restore missing files from backup
  backup-slurm-files.py -v                   # Verbose output
""",
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for backup (default: ./slurm-files-YYYYMMDD_HHMMSS)",
    )
    parser.add_argument(
        "--restore",
        type=str,
        metavar="BACKUP_DIR",
        help="Restore missing files from the specified backup directory",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    if os.geteuid() != 0:
        print(
            f"{Colors.YELLOW}Warning: not running as root; some files may not be readable/writable.{Colors.RESET}"
        )

    if args.restore:
        # Restore mode
        restore = SlurmFilesRestore(backup_dir=args.restore, verbose=args.verbose)
        restore.run_restore()
    else:
        # Backup mode
        if args.output_dir:
            output_dir = args.output_dir
        else:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = f"./slurm-files-{ts}"

        backup = SlurmFilesBackup(output_dir=output_dir, verbose=args.verbose)
        backup.run_backup()


if __name__ == "__main__":
    main()
