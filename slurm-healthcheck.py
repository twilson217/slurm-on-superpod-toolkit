#!/usr/bin/env python3
"""
Slurm Cluster Healthcheck Script

Comprehensive healthcheck tool for validating Slurm cluster health before and after upgrades.
Supports baseline capture, post-upgrade comparison, and detailed validation of all components.

Usage:
    ./slurm-healthcheck.py                                    # Run standard healthcheck
    ./slurm-healthcheck.py --pre-upgrade -o baseline.json     # Capture pre-upgrade baseline
    ./slurm-healthcheck.py --post-upgrade -b baseline.json    # Compare post-upgrade to baseline
    ./slurm-healthcheck.py -v                                 # Verbose output
    ./slurm-healthcheck.py --json                             # JSON output format
"""

import argparse
import json
import subprocess
import sys
import os
import time
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum


class TestStatus(Enum):
    """Test result status"""
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    SKIP = "SKIP"


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    RESET = '\033[0m'
    
    @classmethod
    def disable(cls):
        """Disable colors for non-TTY output"""
        cls.GREEN = cls.YELLOW = cls.RED = cls.BLUE = cls.CYAN = cls.BOLD = cls.RESET = ''


@dataclass
class TestResult:
    """Individual test result"""
    category: str
    name: str
    status: TestStatus
    message: str = ""
    details: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}


@dataclass
class HealthcheckResults:
    """Overall healthcheck results"""
    timestamp: str
    hostname: str
    slurm_version: str
    user: str
    tests: List[TestResult]
    
    def summary(self) -> Dict[str, int]:
        """Calculate test summary statistics"""
        return {
            'total': len(self.tests),
            'passed': sum(1 for t in self.tests if t.status == TestStatus.PASS),
            'failed': sum(1 for t in self.tests if t.status == TestStatus.FAIL),
            'warnings': sum(1 for t in self.tests if t.status == TestStatus.WARN),
            'skipped': sum(1 for t in self.tests if t.status == TestStatus.SKIP),
        }
    
    def overall_status(self) -> str:
        """Determine overall cluster health status"""
        summary = self.summary()
        if summary['failed'] > 0:
            return "CRITICAL"
        elif summary['warnings'] > 0:
            return "DEGRADED"
        else:
            return "HEALTHY"


class SlurmHealthcheck:
    """Main healthcheck class"""
    
    def __init__(self, verbose: bool = False, quiet: bool = False, use_colors: bool = True):
        self.verbose = verbose
        self.quiet = quiet
        self.results: List[TestResult] = []
        self.bcm_version = None
        self.slurm_base_path = None
        self.controller_nodes = []
        self.accounting_nodes = []
        self.cmsh_path = None
        
        if not use_colors or not sys.stdout.isatty():
            Colors.disable()
        
        # Detect BCM environment
        self._detect_bcm_environment()
    
    def run_command(self, cmd: List[str], timeout: int = 30, check: bool = False) -> Tuple[int, str, str]:
        """Run a shell command with timeout and error handling"""
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=check
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return -1, "", f"Command timed out after {timeout} seconds"
        except FileNotFoundError:
            return -1, "", f"Command not found: {cmd[0]}"
        except Exception as e:
            return -1, "", str(e)
    
    def _detect_bcm_environment(self):
        """Detect BCM version and configuration"""
        # Detect BCM version
        cmd_conf = '/cm/local/apps/cmd/etc/cmd.conf'
        if os.path.exists(cmd_conf):
            returncode, stdout, _ = self.run_command(['grep', 'VERSION', cmd_conf])
            if returncode == 0:
                match = re.search(r'VERSION\s+(\d+)\.', stdout)
                if match:
                    self.bcm_version = int(match.group(1))
        
        # Determine Slurm base path based on BCM version
        if self.bcm_version == 10:
            self.slurm_base_path = '/cm/shared/apps/slurm'
        elif self.bcm_version and self.bcm_version >= 11:
            self.slurm_base_path = '/cm/local/apps/slurm'
        else:
            # Try to auto-detect
            if os.path.exists('/cm/shared/apps/slurm/current'):
                self.slurm_base_path = '/cm/shared/apps/slurm'
                self.bcm_version = 10
            elif os.path.exists('/cm/local/apps/slurm/current'):
                self.slurm_base_path = '/cm/local/apps/slurm'
                self.bcm_version = 11
        
        # Find cmsh
        cmsh_locations = [
            '/cm/local/apps/cmd/bin/cmsh',
            '/usr/bin/cmsh',
        ]
        for location in cmsh_locations:
            if os.path.exists(location):
                self.cmsh_path = location
                break
        
        # Discover controller and accounting nodes via cmsh
        if self.cmsh_path:
            self._discover_slurm_nodes()
    
    def _discover_slurm_nodes(self):
        """Use cmsh to discover which nodes have slurmserver and slurmaccounting roles"""
        if not self.cmsh_path:
            return
        
        # Method 1: Query device mode to see all nodes and their roles
        # Use: device; show -l | grep -i slurm
        cmd = f'{self.cmsh_path} -c "device; show -l"'
        returncode, stdout, _ = self.run_command(['bash', '-c', cmd], timeout=15)
        if returncode == 0:
            current_node = None
            for line in stdout.split('\n'):
                # Look for node names (lines with node identifiers)
                node_match = re.match(r'^(\S+)\s+', line)
                if node_match and not line.startswith(' '):
                    current_node = node_match.group(1)
                
                # Look for role assignments in the line
                if current_node and 'slurmserver' in line.lower():
                    if current_node not in self.controller_nodes:
                        self.controller_nodes.append(current_node)
                
                if current_node and 'slurmaccounting' in line.lower():
                    if current_node not in self.accounting_nodes:
                        self.accounting_nodes.append(current_node)
        
        # Method 2: Try direct role query
        # Get list of devices with slurmserver role
        cmd = f'{self.cmsh_path} -c "device; list -l slurmserver"'
        returncode, stdout, _ = self.run_command(['bash', '-c', cmd], timeout=10)
        if returncode == 0:
            for line in stdout.split('\n'):
                line = line.strip()
                if line and 'PhysicalNode' in line:
                    # Format: PhysicalNode  nodename  MAC  category  IP  ...
                    # Node name is in the second column
                    parts = line.split()
                    if len(parts) >= 2:
                        node = parts[1]
                        if node and node not in self.controller_nodes:
                            self.controller_nodes.append(node)
        
        # Get list of devices with slurmaccounting role
        cmd = f'{self.cmsh_path} -c "device; list -l slurmaccounting"'
        returncode, stdout, _ = self.run_command(['bash', '-c', cmd], timeout=10)
        if returncode == 0:
            for line in stdout.split('\n'):
                line = line.strip()
                if line and 'PhysicalNode' in line:
                    parts = line.split()
                    if len(parts) >= 2:
                        node = parts[1]
                        if node and node not in self.accounting_nodes:
                            self.accounting_nodes.append(node)
        
        # Method 3: If still no nodes found, try to parse from configurationoverlay
        if not self.controller_nodes:
            cmd = f'{self.cmsh_path} -c "configurationoverlay; show"'
            returncode, stdout, _ = self.run_command(['bash', '-c', cmd], timeout=10)
            if returncode == 0:
                # Look for patterns like "slurmctl-01" or similar in the output
                for match in re.finditer(r'\b([\w-]+(?:ctl|controller|slurm)[\w-]*)\b', stdout, re.IGNORECASE):
                    node = match.group(1)
                    if node not in self.controller_nodes:
                        self.controller_nodes.append(node)
    
    def run_ssh_command(self, node: str, cmd: List[str], timeout: int = 30) -> Tuple[int, str, str]:
        """Run a command on a remote node via SSH"""
        ssh_cmd = ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ConnectTimeout=5', 
                   node] + cmd
        return self.run_command(ssh_cmd, timeout=timeout)
    
    def add_result(self, category: str, name: str, status: TestStatus, 
                   message: str = "", details: Dict[str, Any] = None):
        """Add a test result"""
        result = TestResult(category, name, status, message, details or {})
        self.results.append(result)
        
        if not self.quiet:
            self._print_result(result)
    
    def _print_result(self, result: TestResult):
        """Print a single test result"""
        status_colors = {
            TestStatus.PASS: Colors.GREEN,
            TestStatus.FAIL: Colors.RED,
            TestStatus.WARN: Colors.YELLOW,
            TestStatus.SKIP: Colors.CYAN,
        }
        
        color = status_colors.get(result.status, Colors.RESET)
        status_str = f"{color}[{result.status.value}]{Colors.RESET}"
        
        print(f"{status_str} {result.category}: {result.name}")
        
        if result.message and (self.verbose or result.status in [TestStatus.FAIL, TestStatus.WARN]):
            print(f"  → {result.message}")
        
        if self.verbose and result.details:
            for key, value in result.details.items():
                print(f"    {key}: {value}")
    
    def print_header(self):
        """Print healthcheck header"""
        if self.quiet:
            return
        
        print(f"\n{Colors.BOLD}{'=' * 65}")
        print("SLURM CLUSTER HEALTHCHECK (BCM Environment)")
        print('=' * 65 + Colors.RESET)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Hostname: {os.uname().nodename}")
        print(f"User: {os.getenv('USER', 'unknown')}")
        
        # BCM info
        if self.bcm_version:
            print(f"BCM Version: {self.bcm_version}.x")
        if self.slurm_base_path:
            print(f"Slurm Base Path: {self.slurm_base_path}")
        if self.controller_nodes:
            print(f"Controller Node(s): {', '.join(self.controller_nodes)}")
        
        # Get Slurm version
        returncode, stdout, _ = self.run_command(['sinfo', '--version'])
        if returncode == 0:
            print(f"Slurm Version: {stdout.strip()}")
        
        print(f"{Colors.BOLD}{'=' * 65}{Colors.RESET}\n")
    
    def print_summary(self, results: HealthcheckResults):
        """Print healthcheck summary"""
        if self.quiet:
            return
        
        summary = results.summary()
        overall = results.overall_status()
        
        print(f"\n{Colors.BOLD}{'=' * 65}")
        print("SUMMARY")
        print('=' * 65 + Colors.RESET)
        print(f"Total Tests: {summary['total']}")
        print(f"{Colors.GREEN}Passed: {summary['passed']}{Colors.RESET}")
        print(f"{Colors.RED}Failed: {summary['failed']}{Colors.RESET}")
        print(f"{Colors.YELLOW}Warnings: {summary['warnings']}{Colors.RESET}")
        print(f"{Colors.CYAN}Skipped: {summary['skipped']}{Colors.RESET}")
        
        if overall == "HEALTHY":
            color = Colors.GREEN
        elif overall == "DEGRADED":
            color = Colors.YELLOW
        else:
            color = Colors.RED
        
        print(f"\n{Colors.BOLD}Overall Status: {color}{overall}{Colors.RESET}")
        print(f"{Colors.BOLD}{'=' * 65}{Colors.RESET}\n")
    
    # =========================================================================
    # CHECK FUNCTIONS
    # =========================================================================
    
    def check_slurm_version(self) -> Optional[str]:
        """Check Slurm version and availability"""
        returncode, stdout, stderr = self.run_command(['sinfo', '--version'])
        
        if returncode == 0:
            version = stdout.strip()
            self.add_result(
                "System", "Slurm Version Check",
                TestStatus.PASS,
                f"Slurm is installed: {version}",
                {"version": version}
            )
            return version
        else:
            self.add_result(
                "System", "Slurm Version Check",
                TestStatus.FAIL,
                f"Unable to determine Slurm version: {stderr}",
                {"error": stderr}
            )
            return None
    
    def check_services(self):
        """Check status of Slurm services on controller/accounting nodes"""
        # Check slurmctld on controller nodes
        if self.controller_nodes:
            for node in self.controller_nodes:
                returncode, stdout, stderr = self.run_ssh_command(
                    node,
                    ['systemctl', 'is-active', 'slurmctld.service']
                )
                
                is_active = stdout.strip() == 'active'
                
                if is_active:
                    # Get uptime info
                    _, uptime_out, _ = self.run_ssh_command(
                        node,
                        ['systemctl', 'show', 'slurmctld.service', '--property=ActiveEnterTimestamp']
                    )
                    
                    self.add_result(
                        "Services", f"Slurm Controller on {node}",
                        TestStatus.PASS,
                        f"slurmctld is active on {node}",
                        {"node": node, "status": "active", "details": uptime_out.strip()}
                    )
                else:
                    self.add_result(
                        "Services", f"Slurm Controller on {node}",
                        TestStatus.FAIL,
                        f"slurmctld is not active on {node}: {stdout.strip()}",
                        {"node": node, "status": stdout.strip()}
                    )
        else:
            self.add_result(
                "Services", "Slurm Controller Discovery",
                TestStatus.WARN,
                "Could not discover controller nodes via cmsh",
                {}
            )
        
        # Check slurmdbd on accounting nodes
        if self.accounting_nodes:
            for node in self.accounting_nodes:
                returncode, stdout, stderr = self.run_ssh_command(
                    node,
                    ['systemctl', 'is-active', 'slurmdbd.service']
                )
                
                is_active = stdout.strip() == 'active'
                
                if is_active:
                    _, uptime_out, _ = self.run_ssh_command(
                        node,
                        ['systemctl', 'show', 'slurmdbd.service', '--property=ActiveEnterTimestamp']
                    )
                    
                    self.add_result(
                        "Services", f"Slurm Database on {node}",
                        TestStatus.PASS,
                        f"slurmdbd is active on {node}",
                        {"node": node, "status": "active", "details": uptime_out.strip()}
                    )
                else:
                    self.add_result(
                        "Services", f"Slurm Database on {node}",
                        TestStatus.FAIL,
                        f"slurmdbd is not active on {node}: {stdout.strip()}",
                        {"node": node, "status": stdout.strip()}
                    )
        else:
            self.add_result(
                "Services", "Slurm Database Discovery",
                TestStatus.WARN,
                "Could not discover accounting nodes via cmsh",
                {}
            )
    
    def check_nodes(self) -> Dict[str, Any]:
        """Check compute node status"""
        returncode, stdout, stderr = self.run_command(['sinfo', '-N', '-h', '-o', '%N|%T|%E'])
        
        if returncode != 0:
            self.add_result(
                "Nodes", "Node Status Check",
                TestStatus.FAIL,
                f"Unable to query node status: {stderr}",
                {"error": stderr}
            )
            return {}
        
        nodes = {}
        state_counts = {}
        problem_nodes = []
        
        for line in stdout.strip().split('\n'):
            if not line:
                continue
            
            parts = line.split('|')
            if len(parts) >= 2:
                node_name = parts[0]
                state = parts[1]
                reason = parts[2] if len(parts) > 2 else ""
                
                nodes[node_name] = {"state": state, "reason": reason}
                state_counts[state] = state_counts.get(state, 0) + 1
                
                # Track problem nodes
                if state.lower() not in ['idle', 'allocated', 'mixed', 'completing']:
                    problem_nodes.append((node_name, state, reason))
        
        total_nodes = len(nodes)
        
        # Determine status
        if problem_nodes:
            status = TestStatus.WARN if len(problem_nodes) < total_nodes * 0.1 else TestStatus.FAIL
            message = f"{len(problem_nodes)} of {total_nodes} nodes have issues"
            if self.verbose or status == TestStatus.FAIL:
                for node, state, reason in problem_nodes[:5]:  # Show first 5
                    message += f"\n    {node}: {state} ({reason})"
        else:
            status = TestStatus.PASS
            message = f"All {total_nodes} nodes are healthy"
        
        self.add_result(
            "Nodes", "Node Health Check",
            status,
            message,
            {"total_nodes": total_nodes, "state_counts": state_counts, "problem_count": len(problem_nodes)}
        )
        
        return {"nodes": nodes, "state_counts": state_counts, "total": total_nodes}
    
    def check_slurmdbd_connection(self):
        """Check slurmdbd connectivity and accounting"""
        returncode, stdout, stderr = self.run_command(['sacctmgr', 'show', 'cluster', '-n'], timeout=10)
        
        if returncode == 0 and stdout.strip():
            clusters = [line.split()[0] for line in stdout.strip().split('\n') if line.strip()]
            self.add_result(
                "Accounting", "Database Connection",
                TestStatus.PASS,
                f"slurmdbd is accessible, found {len(clusters)} cluster(s)",
                {"clusters": clusters}
            )
        else:
            self.add_result(
                "Accounting", "Database Connection",
                TestStatus.FAIL,
                f"Unable to connect to slurmdbd: {stderr}",
                {"error": stderr}
            )
    
    def check_job_history(self):
        """Check that job accounting history is accessible"""
        returncode, stdout, stderr = self.run_command(
            ['sacct', '-S', 'now-7days', '-n', '--format=JobID', '--state=COMPLETED,FAILED,CANCELLED'],
            timeout=15
        )
        
        if returncode == 0:
            job_count = len([line for line in stdout.strip().split('\n') if line.strip()])
            self.add_result(
                "Accounting", "Job History Access",
                TestStatus.PASS,
                f"Job history accessible ({job_count} jobs in last 7 days)",
                {"recent_job_count": job_count}
            )
        else:
            status = TestStatus.WARN if "No jobs" in stderr else TestStatus.FAIL
            self.add_result(
                "Accounting", "Job History Access",
                status,
                f"Issue accessing job history: {stderr}",
                {"error": stderr}
            )
    
    def check_partitions(self) -> Dict[str, Any]:
        """Check partition configuration"""
        returncode, stdout, stderr = self.run_command(['sinfo', '-h', '-o', '%R|%a|%l|%D|%T'])
        
        if returncode != 0:
            self.add_result(
                "Configuration", "Partition Check",
                TestStatus.FAIL,
                f"Unable to query partitions: {stderr}",
                {"error": stderr}
            )
            return {}
        
        partitions = {}
        for line in stdout.strip().split('\n'):
            if not line:
                continue
            parts = line.split('|')
            if len(parts) >= 4:
                partition = parts[0]
                available = parts[1]
                timelimit = parts[2]
                nodes = parts[3]
                state = parts[4] if len(parts) > 4 else ""
                
                if partition not in partitions:
                    partitions[partition] = {
                        "available": available,
                        "timelimit": timelimit,
                        "nodes": nodes,
                        "state": state
                    }
        
        if partitions:
            self.add_result(
                "Configuration", "Partition Check",
                TestStatus.PASS,
                f"Found {len(partitions)} partition(s)",
                {"partitions": list(partitions.keys())}
            )
        else:
            self.add_result(
                "Configuration", "Partition Check",
                TestStatus.WARN,
                "No partitions found",
                {}
            )
        
        return partitions
    
    def check_job_submission(self):
        """Test basic job submission"""
        # Use /cm/shared for test script (available to all nodes)
        test_script = '/cm/shared/slurm_healthcheck_test.sh'
        
        # Create test script
        try:
            with open(test_script, 'w') as f:
                f.write('#!/bin/bash\necho "Healthcheck test job"\nhostname\ndate\n')
            os.chmod(test_script, 0o755)
        except Exception as e:
            self.add_result(
                "Job Submission", "Basic Job Test",
                TestStatus.SKIP,
                f"Unable to create test script: {e}",
                {}
            )
            return
        
        # Submit test job with explicit working directory
        start_time = time.time()
        returncode, stdout, stderr = self.run_command(
            ['srun', '--overlap', '-t', '00:01:00', '-D', '/tmp', test_script],
            timeout=120
        )
        elapsed = time.time() - start_time
        
        # Cleanup
        try:
            os.remove(test_script)
        except:
            pass
        
        if returncode == 0:
            self.add_result(
                "Job Submission", "Basic Job Test",
                TestStatus.PASS,
                f"Job submission successful (elapsed: {elapsed:.2f}s)",
                {"elapsed_seconds": elapsed, "output": stdout.strip()}
            )
        else:
            self.add_result(
                "Job Submission", "Basic Job Test",
                TestStatus.FAIL,
                f"Job submission failed: {stderr}",
                {"error": stderr, "elapsed_seconds": elapsed}
            )
    
    def check_pyxis(self):
        """Check Pyxis/Enroot configuration"""
        # Check if Pyxis plugin exists - use detected BCM path
        pyxis_paths = []
        
        if self.slurm_base_path:
            # Check both lib64 and lib (BCM typically uses lib64)
            pyxis_paths.append(f'{self.slurm_base_path}/current/lib64/slurm/spank_pyxis.so')
            pyxis_paths.append(f'{self.slurm_base_path}/current/lib/slurm/spank_pyxis.so')
        
        # Fallback paths (check lib64 first, then lib)
        pyxis_paths.extend([
            '/cm/shared/apps/slurm/current/lib64/slurm/spank_pyxis.so',
            '/cm/shared/apps/slurm/current/lib/slurm/spank_pyxis.so',
            '/cm/local/apps/slurm/current/lib64/slurm/spank_pyxis.so',
            '/cm/local/apps/slurm/current/lib/slurm/spank_pyxis.so',
            '/usr/lib64/slurm/spank_pyxis.so',
            '/usr/lib/slurm/spank_pyxis.so',
        ])
        
        pyxis_found = False
        pyxis_path = None
        
        for path in pyxis_paths:
            if os.path.exists(path):
                pyxis_found = True
                pyxis_path = path
                break
        
        if not pyxis_found:
            self.add_result(
                "Pyxis", "Pyxis Installation",
                TestStatus.SKIP,
                "Pyxis plugin not found (may not be installed)",
                {"checked_paths": pyxis_paths[:3]}
            )
            return
        
        # Check enroot
        returncode, stdout, stderr = self.run_command(['which', 'enroot'])
        
        if returncode == 0:
            enroot_path = stdout.strip()
            
            # Get enroot version
            ret, ver_out, _ = self.run_command(['enroot', 'version'])
            version = ver_out.strip() if ret == 0 else "unknown"
            
            self.add_result(
                "Pyxis", "Enroot Installation",
                TestStatus.PASS,
                f"Enroot found at {enroot_path}",
                {"path": enroot_path, "version": version}
            )
            
            # Test container job (lightweight test, skip actual pull)
            self.add_result(
                "Pyxis", "Pyxis Plugin",
                TestStatus.PASS,
                f"Pyxis plugin found at {pyxis_path}",
                {"plugin_path": pyxis_path}
            )
        else:
            self.add_result(
                "Pyxis", "Enroot Installation",
                TestStatus.WARN,
                "Pyxis plugin found but enroot not in PATH",
                {"plugin_path": pyxis_path}
            )
    
    def check_logs(self):
        """Check Slurm logs for recent errors"""
        error_patterns = [r'error', r'fatal', r'critical']
        
        # Check controller logs on controller nodes
        if self.controller_nodes:
            for node in self.controller_nodes[:1]:  # Check first controller only
                # Try journalctl first (common in modern systems)
                returncode, stdout, stderr = self.run_ssh_command(
                    node,
                    ['journalctl', '-u', 'slurmctld', '-n', '100', '--no-pager']
                )
                
                if returncode != 0:
                    # Fallback to log file
                    log_file = '/var/log/slurm/slurmctld.log'
                    returncode, stdout, stderr = self.run_ssh_command(
                        node,
                        ['tail', '-n', '100', log_file]
                    )
                    
                    if returncode != 0:
                        self.add_result(
                            "Logs", f"Controller Log on {node}",
                            TestStatus.SKIP,
                            f"Unable to read logs (tried journalctl and {log_file})",
                            {}
                        )
                        continue
                
                error_lines = []
                for line in stdout.split('\n'):
                    for pattern in error_patterns:
                        if re.search(pattern, line, re.IGNORECASE):
                            error_lines.append(line.strip())
                            break
                
                if error_lines:
                    status = TestStatus.WARN
                    message = f"Found {len(error_lines)} error/warning line(s) in last 100 lines"
                    if self.verbose:
                        message += "\n    " + "\n    ".join(error_lines[:3])
                else:
                    status = TestStatus.PASS
                    message = "No recent errors found"
                
                self.add_result(
                    "Logs", f"Controller Log on {node}",
                    status,
                    message,
                    {"error_count": len(error_lines), "node": node}
                )
        
        # Check database logs on accounting nodes
        if self.accounting_nodes:
            for node in self.accounting_nodes[:1]:  # Check first accounting node only
                # Try journalctl first
                returncode, stdout, stderr = self.run_ssh_command(
                    node,
                    ['journalctl', '-u', 'slurmdbd', '-n', '100', '--no-pager']
                )
                
                if returncode != 0:
                    # Fallback to log file
                    log_file = '/var/log/slurm/slurmdbd.log'
                    returncode, stdout, stderr = self.run_ssh_command(
                        node,
                        ['tail', '-n', '100', log_file]
                    )
                    
                    if returncode != 0:
                        self.add_result(
                            "Logs", f"Database Log on {node}",
                            TestStatus.SKIP,
                            f"Unable to read logs (tried journalctl and {log_file})",
                            {}
                        )
                        continue
                
                error_lines = []
                for line in stdout.split('\n'):
                    for pattern in error_patterns:
                        if re.search(pattern, line, re.IGNORECASE):
                            error_lines.append(line.strip())
                            break
                
                if error_lines:
                    status = TestStatus.WARN
                    message = f"Found {len(error_lines)} error/warning line(s) in last 100 lines"
                    if self.verbose:
                        message += "\n    " + "\n    ".join(error_lines[:3])
                else:
                    status = TestStatus.PASS
                    message = "No recent errors found"
                
                self.add_result(
                    "Logs", f"Database Log on {node}",
                    status,
                    message,
                    {"error_count": len(error_lines), "node": node}
                )
    
    def check_munge(self):
        """Check munge authentication service"""
        returncode, stdout, stderr = self.run_command(['systemctl', 'is-active', 'munge.service'])
        
        if stdout.strip() == 'active':
            # Test munge encode/decode
            ret, encoded, _ = self.run_command(['munge', '-n'], timeout=5)
            if ret == 0 and encoded:
                ret2, decoded, err = self.run_command(['unmunge'], timeout=5, check=False)
                # Pass encoded data to unmunge via stdin
                ret2, decoded, err = self.run_command(['bash', '-c', f'echo "{encoded.strip()}" | unmunge'], timeout=5)
                
                if ret2 == 0:
                    status = TestStatus.PASS
                    message = "Munge authentication working"
                else:
                    status = TestStatus.WARN
                    message = f"Munge encode works but decode failed"
            else:
                status = TestStatus.WARN
                message = "Munge service active but encode test failed"
            
            self.add_result(
                "Authentication", "Munge Service",
                status,
                message,
                {}
            )
        else:
            self.add_result(
                "Authentication", "Munge Service",
                TestStatus.FAIL,
                f"Munge is not active: {stdout.strip()}",
                {"status": stdout.strip()}
            )
    
    # =========================================================================
    # BASELINE CAPTURE AND COMPARISON
    # =========================================================================
    
    def capture_baseline(self) -> Dict[str, Any]:
        """Capture comprehensive baseline state for pre-upgrade"""
        print(f"{Colors.BOLD}Capturing pre-upgrade baseline...{Colors.RESET}\n")
        
        baseline = {
            'timestamp': datetime.now().isoformat(),
            'hostname': os.uname().nodename,
            'user': os.getenv('USER', 'unknown'),
            'slurm_version': None,
            'accounting': {},
            'configuration': {},
            'system_state': {},
        }
        
        # Slurm version
        ret, out, _ = self.run_command(['sinfo', '--version'])
        if ret == 0:
            baseline['slurm_version'] = out.strip()
            print(f"✓ Captured Slurm version: {out.strip()}")
        
        # Accounting data
        accounting_commands = {
            'users': ['sacctmgr', 'show', 'user', '-P', '-n'],
            'accounts': ['sacctmgr', 'show', 'account', '-P', '-n'],
            'qos': ['sacctmgr', 'show', 'qos', '-P', '-n'],
            'associations': ['sacctmgr', 'show', 'associations', '-P', '-n'],
            'tres': ['sacctmgr', 'show', 'tres', '-P', '-n'],
            'clusters': ['sacctmgr', 'show', 'cluster', '-P', '-n'],
        }
        
        for key, cmd in accounting_commands.items():
            ret, out, err = self.run_command(cmd, timeout=30)
            if ret == 0:
                lines = [line for line in out.strip().split('\n') if line.strip()]
                baseline['accounting'][key] = lines
                print(f"✓ Captured {len(lines)} {key}")
            else:
                baseline['accounting'][key] = []
                print(f"✗ Failed to capture {key}: {err}")
        
        # Job history stats
        ret, out, _ = self.run_command(['sacct', '-S', 'now-30days', '-n', '--format=JobID,User,Account'], timeout=60)
        if ret == 0:
            jobs = [line.strip() for line in out.strip().split('\n') if line.strip()]
            baseline['accounting']['job_count_30days'] = len(jobs)
            
            # Count per user and account
            user_counts = {}
            account_counts = {}
            for line in jobs:
                parts = line.split()
                if len(parts) >= 3:
                    user = parts[1]
                    account = parts[2]
                    user_counts[user] = user_counts.get(user, 0) + 1
                    account_counts[account] = account_counts.get(account, 0) + 1
            
            baseline['accounting']['user_job_counts'] = user_counts
            baseline['accounting']['account_job_counts'] = account_counts
            print(f"✓ Captured job history: {len(jobs)} jobs in last 30 days")
        
        # Configuration state
        ret, out, _ = self.run_command(['sinfo', '-h', '-o', '%R|%a|%l|%D'])
        if ret == 0:
            partitions = {}
            for line in out.strip().split('\n'):
                if line.strip():
                    parts = line.split('|')
                    if len(parts) >= 4:
                        partitions[parts[0]] = {
                            'available': parts[1],
                            'timelimit': parts[2],
                            'nodes': parts[3]
                        }
            baseline['configuration']['partitions'] = partitions
            print(f"✓ Captured {len(partitions)} partition(s)")
        
        # Node configuration
        ret, out, _ = self.run_command(['sinfo', '-N', '-h', '-o', '%N|%T|%G'])
        if ret == 0:
            nodes = {}
            for line in out.strip().split('\n'):
                if line.strip():
                    parts = line.split('|')
                    if len(parts) >= 2:
                        nodes[parts[0]] = {
                            'state': parts[1],
                            'gres': parts[2] if len(parts) > 2 else ""
                        }
            baseline['configuration']['nodes'] = nodes
            baseline['system_state']['total_nodes'] = len(nodes)
            print(f"✓ Captured {len(nodes)} node(s)")
        
        # State counts
        ret, out, _ = self.run_command(['sinfo', '-h', '-o', '%T'])
        if ret == 0:
            states = [s.strip() for s in out.strip().split('\n') if s.strip()]
            state_counts = {}
            for state in states:
                state_counts[state] = state_counts.get(state, 0) + 1
            baseline['system_state']['node_state_counts'] = state_counts
            print(f"✓ Captured node state distribution")
        
        print(f"\n{Colors.GREEN}{Colors.BOLD}✓ Baseline capture complete{Colors.RESET}\n")
        
        return baseline
    
    def compare_baseline(self, baseline: Dict[str, Any]):
        """Compare current state to baseline"""
        print(f"{Colors.BOLD}Comparing current state to baseline...{Colors.RESET}\n")
        
        # Compare versions
        ret, current_version, _ = self.run_command(['sinfo', '--version'])
        if ret == 0:
            current_version = current_version.strip()
            baseline_version = baseline.get('slurm_version', 'unknown')
            
            if current_version == baseline_version:
                self.add_result(
                    "Baseline Comparison", "Version Check",
                    TestStatus.WARN,
                    f"Version unchanged: {current_version} (expected upgrade)",
                    {"baseline": baseline_version, "current": current_version}
                )
            else:
                self.add_result(
                    "Baseline Comparison", "Version Check",
                    TestStatus.PASS,
                    f"Version upgraded: {baseline_version} → {current_version}",
                    {"baseline": baseline_version, "current": current_version}
                )
        
        # Compare accounting data
        accounting_baseline = baseline.get('accounting', {})
        
        for data_type in ['users', 'accounts', 'qos', 'tres', 'clusters']:
            baseline_items = accounting_baseline.get(data_type, [])
            baseline_count = len(baseline_items)
            
            # Get current data
            cmd_map = {
                'users': ['sacctmgr', 'show', 'user', '-P', '-n'],
                'accounts': ['sacctmgr', 'show', 'account', '-P', '-n'],
                'qos': ['sacctmgr', 'show', 'qos', '-P', '-n'],
                'tres': ['sacctmgr', 'show', 'tres', '-P', '-n'],
                'clusters': ['sacctmgr', 'show', 'cluster', '-P', '-n'],
            }
            
            ret, out, err = self.run_command(cmd_map[data_type], timeout=30)
            if ret == 0:
                current_items = [line for line in out.strip().split('\n') if line.strip()]
                current_count = len(current_items)
                
                if current_count < baseline_count:
                    self.add_result(
                        "Baseline Comparison", f"Accounting: {data_type.title()}",
                        TestStatus.FAIL,
                        f"DATA LOSS: {baseline_count} → {current_count} ({baseline_count - current_count} lost)",
                        {"baseline_count": baseline_count, "current_count": current_count}
                    )
                elif current_count == baseline_count:
                    self.add_result(
                        "Baseline Comparison", f"Accounting: {data_type.title()}",
                        TestStatus.PASS,
                        f"Count preserved: {current_count} {data_type}",
                        {"baseline_count": baseline_count, "current_count": current_count}
                    )
                else:
                    self.add_result(
                        "Baseline Comparison", f"Accounting: {data_type.title()}",
                        TestStatus.PASS,
                        f"Count increased: {baseline_count} → {current_count}",
                        {"baseline_count": baseline_count, "current_count": current_count}
                    )
            else:
                self.add_result(
                    "Baseline Comparison", f"Accounting: {data_type.title()}",
                    TestStatus.FAIL,
                    f"Unable to query current {data_type}: {err}",
                    {}
                )
        
        # Compare job counts
        baseline_job_count = accounting_baseline.get('job_count_30days', 0)
        ret, out, _ = self.run_command(['sacct', '-S', 'now-30days', '-n', '--format=JobID'], timeout=60)
        if ret == 0:
            current_job_count = len([line for line in out.strip().split('\n') if line.strip()])
            
            if current_job_count < baseline_job_count * 0.9:  # Allow 10% variance
                self.add_result(
                    "Baseline Comparison", "Job History Integrity",
                    TestStatus.FAIL,
                    f"Significant job count decrease: {baseline_job_count} → {current_job_count}",
                    {"baseline": baseline_job_count, "current": current_job_count}
                )
            else:
                self.add_result(
                    "Baseline Comparison", "Job History Integrity",
                    TestStatus.PASS,
                    f"Job history intact: {current_job_count} jobs (baseline: {baseline_job_count})",
                    {"baseline": baseline_job_count, "current": current_job_count}
                )
        
        # Compare partitions
        baseline_partitions = baseline.get('configuration', {}).get('partitions', {})
        ret, out, _ = self.run_command(['sinfo', '-h', '-o', '%R'])
        if ret == 0:
            current_partitions = set(line.strip() for line in out.strip().split('\n') if line.strip())
            baseline_partition_names = set(baseline_partitions.keys())
            
            missing = baseline_partition_names - current_partitions
            new = current_partitions - baseline_partition_names
            
            if missing:
                self.add_result(
                    "Baseline Comparison", "Partition Configuration",
                    TestStatus.FAIL,
                    f"Missing partitions: {', '.join(missing)}",
                    {"missing": list(missing), "new": list(new)}
                )
            elif new:
                self.add_result(
                    "Baseline Comparison", "Partition Configuration",
                    TestStatus.PASS,
                    f"All partitions preserved, {len(new)} new partition(s) added",
                    {"new": list(new)}
                )
            else:
                self.add_result(
                    "Baseline Comparison", "Partition Configuration",
                    TestStatus.PASS,
                    f"All {len(current_partitions)} partitions preserved",
                    {}
                )
        
        # Compare node count
        baseline_node_count = baseline.get('system_state', {}).get('total_nodes', 0)
        ret, out, _ = self.run_command(['sinfo', '-N', '-h'])
        if ret == 0:
            current_node_count = len([line for line in out.strip().split('\n') if line.strip()])
            
            if current_node_count < baseline_node_count:
                self.add_result(
                    "Baseline Comparison", "Node Count",
                    TestStatus.FAIL,
                    f"Node count decreased: {baseline_node_count} → {current_node_count}",
                    {"baseline": baseline_node_count, "current": current_node_count}
                )
            else:
                self.add_result(
                    "Baseline Comparison", "Node Count",
                    TestStatus.PASS,
                    f"Node count preserved: {current_node_count} nodes",
                    {"baseline": baseline_node_count, "current": current_node_count}
                )
    
    # =========================================================================
    # MAIN EXECUTION
    # =========================================================================
    
    def run_all_checks(self):
        """Run all healthcheck tests"""
        self.check_slurm_version()
        self.check_services()
        self.check_nodes()
        self.check_slurmdbd_connection()
        self.check_job_history()
        self.check_partitions()
        self.check_munge()
        self.check_logs()
        self.check_pyxis()
        self.check_job_submission()
    
    def get_results(self) -> HealthcheckResults:
        """Get comprehensive results"""
        ret, version_out, _ = self.run_command(['sinfo', '--version'])
        version = version_out.strip() if ret == 0 else "unknown"
        
        return HealthcheckResults(
            timestamp=datetime.now().isoformat(),
            hostname=os.uname().nodename,
            slurm_version=version,
            user=os.getenv('USER', 'unknown'),
            tests=self.results
        )


def main():
    parser = argparse.ArgumentParser(
        description='Slurm Cluster Healthcheck Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                      # Run standard healthcheck
  %(prog)s --pre-upgrade -o baseline.json       # Capture baseline before upgrade
  %(prog)s --post-upgrade -b baseline.json      # Compare after upgrade
  %(prog)s -v                                   # Verbose output
  %(prog)s --json -o results.json               # JSON output
        """
    )
    
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose output with detailed information')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='Quiet mode, only show summary')
    parser.add_argument('--json', action='store_true',
                        help='Output results in JSON format')
    parser.add_argument('--no-color', action='store_true',
                        help='Disable colored output')
    parser.add_argument('-o', '--output', type=str,
                        help='Save results to file')
    parser.add_argument('--pre-upgrade', action='store_true',
                        help='Capture pre-upgrade baseline state')
    parser.add_argument('--post-upgrade', action='store_true',
                        help='Run post-upgrade validation with baseline comparison')
    parser.add_argument('-b', '--baseline', type=str,
                        help='Baseline file for comparison (used with --post-upgrade)')
    parser.add_argument('--compare-only', action='store_true',
                        help='Only run baseline comparison, skip other checks')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.post_upgrade and not args.baseline:
        print("Error: --post-upgrade requires --baseline <file>", file=sys.stderr)
        sys.exit(2)
    
    if args.compare_only and not args.baseline:
        print("Error: --compare-only requires --baseline <file>", file=sys.stderr)
        sys.exit(2)
    
    # Initialize healthcheck
    use_colors = not args.no_color and not args.json
    healthcheck = SlurmHealthcheck(
        verbose=args.verbose,
        quiet=args.quiet or args.json,
        use_colors=use_colors
    )
    
    # Pre-upgrade baseline capture
    if args.pre_upgrade:
        baseline = healthcheck.capture_baseline()
        
        output_file = args.output or f"slurm-baseline-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
        with open(output_file, 'w') as f:
            json.dump(baseline, f, indent=2)
        
        print(f"{Colors.GREEN}✓ Baseline saved to: {output_file}{Colors.RESET}")
        sys.exit(0)
    
    # Print header
    if not args.json:
        healthcheck.print_header()
    
    # Post-upgrade comparison
    if args.post_upgrade or args.compare_only:
        try:
            with open(args.baseline, 'r') as f:
                baseline = json.load(f)
            
            if not args.quiet:
                print(f"Loaded baseline from: {args.baseline}")
                print(f"Baseline timestamp: {baseline.get('timestamp', 'unknown')}")
                print(f"Baseline version: {baseline.get('slurm_version', 'unknown')}\n")
            
            healthcheck.compare_baseline(baseline)
            
        except FileNotFoundError:
            print(f"Error: Baseline file not found: {args.baseline}", file=sys.stderr)
            sys.exit(2)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in baseline file: {e}", file=sys.stderr)
            sys.exit(2)
    
    # Run standard checks (unless compare-only mode)
    if not args.compare_only:
        healthcheck.run_all_checks()
    
    # Get results
    results = healthcheck.get_results()
    
    # Output results
    if args.json:
        # Convert results to dict
        results_dict = {
            'timestamp': results.timestamp,
            'hostname': results.hostname,
            'slurm_version': results.slurm_version,
            'user': results.user,
            'summary': results.summary(),
            'overall_status': results.overall_status(),
            'tests': [
                {
                    'category': t.category,
                    'name': t.name,
                    'status': t.status.value,
                    'message': t.message,
                    'details': t.details
                }
                for t in results.tests
            ]
        }
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results_dict, f, indent=2)
        else:
            print(json.dumps(results_dict, indent=2))
    else:
        # Print summary
        healthcheck.print_summary(results)
        
        # Save to file if requested
        if args.output:
            with open(args.output, 'w') as f:
                f.write(f"Slurm Healthcheck Results\n")
                f.write(f"{'=' * 65}\n")
                f.write(f"Timestamp: {results.timestamp}\n")
                f.write(f"Hostname: {results.hostname}\n")
                f.write(f"Slurm Version: {results.slurm_version}\n")
                f.write(f"User: {results.user}\n\n")
                
                for test in results.tests:
                    f.write(f"[{test.status.value}] {test.category}: {test.name}\n")
                    if test.message:
                        f.write(f"  {test.message}\n")
                    if test.details:
                        for key, value in test.details.items():
                            f.write(f"    {key}: {value}\n")
                    f.write("\n")
                
                summary = results.summary()
                f.write(f"\nSummary:\n")
                f.write(f"  Total: {summary['total']}\n")
                f.write(f"  Passed: {summary['passed']}\n")
                f.write(f"  Failed: {summary['failed']}\n")
                f.write(f"  Warnings: {summary['warnings']}\n")
                f.write(f"  Skipped: {summary['skipped']}\n")
                f.write(f"\nOverall Status: {results.overall_status()}\n")
            
            print(f"Results saved to: {args.output}")
    
    # Exit with appropriate code
    summary = results.summary()
    if summary['failed'] > 0:
        sys.exit(2)
    elif summary['warnings'] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()

