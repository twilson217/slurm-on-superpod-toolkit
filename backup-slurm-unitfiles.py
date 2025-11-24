#!/usr/bin/env python3
"""
Backup Slurm-related systemd unit files from all relevant nodes in a BCM-managed cluster.

This script:
  - Uses cmsh to discover which nodes hold Slurm roles (slurmserver, slurmaccounting,
    slurmclient, slurmsubmit)
  - Uses SSH to query each node for all systemd unit files matching 'slurm*', 'munge*',
    'mysql*', and 'mariadb*' (since these services are critical to Slurm operation)
  - Copies those unit files (and any drop-in directories) into a local backup tree:

      ./slurm-unitfiles/<node>/<systemd-path>/...

Assumptions:
  - Run on a BCM head node as root
  - Passwordless SSH to all Slurm nodes (same as healthcheck assumptions)
  - cmsh available (typically /cm/local/apps/cmd/bin/cmsh)

Usage:
  ./backup-slurm-unitfiles.py
  ./backup-slurm-unitfiles.py -o /path/to/backups
  ./backup-slurm-unitfiles.py -v
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple


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


class SlurmUnitBackup:
    """Main backup orchestrator."""

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

    def discover_nodes_by_role(self):
        """Use cmsh to discover nodes that have various Slurm roles."""
        self.cmsh_path = self.find_cmsh()
        if not self.cmsh_path:
            self.log(
                "ERROR: cmsh binary not found. Ensure BCM is installed and cmsh is available.",
                Colors.RED,
            )
            sys.exit(1)

        # Use 'device; foreach -l <role> (get hostname)' to find nodes per role
        # This avoids hostname truncation that occurs with 'list -l'
        for role in self.nodes_by_role.keys():
            cmd = f'{self.cmsh_path} -c "device; foreach -l {role} (get hostname)"'
            rc, out, err = self.run_local(["bash", "-c", cmd], timeout=20)
            if rc != 0:
                self.log(f"Warning: failed to list devices for role {role}: {err}", Colors.YELLOW)
                continue

            for line in out.splitlines():
                line = line.strip()
                if not line:
                    continue
                # Each line is just a hostname
                self.nodes_by_role[role].add(line)

        # Merge all nodes into a single set
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

    # -------------------------------------------------------------------------
    # Remote systemd inspection and backup
    # -------------------------------------------------------------------------

    def list_slurm_related_units(self, node: str) -> List[str]:
        """List systemd unit files on a node for Slurm and related services.
        
        Includes:
        - slurm* (slurmctld, slurmd, slurmdbd, etc.)
        - munge* (authentication service)
        - mysql* and mariadb* (accounting database, if present)
        """
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
        """Find the full paths of a unit file and any drop-in directories on a remote node.

        Uses `systemctl show` to query FragmentPath and DropInPaths, which is more
        robust than guessing standard directories.
        """
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
                # DropInPaths may contain multiple paths separated by spaces
                dropin_paths.extend(p for p in val.split() if p)

        paths: List[str] = []
        if fragment_path:
            paths.append(fragment_path)
        paths.extend(dropin_paths)

        self.vlog(f"[{node}] Paths for {unit}: {', '.join(paths) if paths else 'none'}")
        return paths

    def backup_remote_path(self, node: str, remote_path: str):
        """
        Backup a single remote path (file or directory) using scp.
        The local path mirrors the remote path under:

            <output_root>/<node>/<remote_path>
        """
        rel_remote = remote_path.lstrip("/")  # e.g. usr/lib/systemd/system/slurmctld.service
        local_dir = self.output_root / node / os.path.dirname(rel_remote)
        local_dir.mkdir(parents=True, exist_ok=True)

        self.vlog(f"[{node}] Backing up {remote_path} -> {local_dir}")

        # Use scp -r to handle both files and directories
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
        else:
            self.vlog(f"[{node}] Copied {remote_path}")

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
                self.backup_remote_path(node, path)

    # -------------------------------------------------------------------------
    # Orchestration
    # -------------------------------------------------------------------------

    def run(self):
        self.log(
            f"{Colors.BOLD}Backing up Slurm and related systemd unit files to: {self.output_root}{Colors.RESET}"
        )
        self.output_root.mkdir(parents=True, exist_ok=True)

        nodes = self.discover_nodes_by_role()

        for node in nodes:
            self.backup_node_units(node)

        self.log(f"\n{Colors.GREEN}{Colors.BOLD}✓ Backup of Slurm and related unit files completed{Colors.RESET}")
        self.log(f"Backup directory: {self.output_root}")


def main():
    parser = argparse.ArgumentParser(
        description="Backup Slurm-related systemd unit files (slurm*, munge*, mysql*, mariadb*) from all relevant nodes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  backup-slurm-unitfiles.py
  backup-slurm-unitfiles.py -o ./slurm-unitfiles-$(date +%Y%m%d)
  backup-slurm-unitfiles.py -v
""",
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default=None,
        help="Output directory (default: ./slurm-unitfiles-YYYYMMDD_HHMMSS)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output",
    )

    args = parser.parse_args()

    # Default output directory if not specified
    if args.output_dir:
        output_dir = args.output_dir
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = f"./slurm-unitfiles-{ts}"

    backup = SlurmUnitBackup(output_dir=output_dir, verbose=args.verbose)

    if os.geteuid() != 0:
        print(
            f"{Colors.YELLOW}Warning: not running as root; some files may not be readable.{Colors.RESET}"
        )

    backup.run()


if __name__ == "__main__":
    main()


