#!/usr/bin/env python3
"""
Migrate Slurm controller (slurmctld) from dedicated nodes to BCM head nodes.

This script is intended to be run on the ACTIVE BCM head node as root.
It will:
  1. Check for PrologSlurmctld/EpilogSlurmctld scripts:
     - If set and NOT on /cm/shared, copy them to BCM head nodes
     - If on /cm/shared (shared storage), no action needed
  2. Find the configuration overlay with the slurm-server (slurmserver) role
  3. Update the overlay to run on all BCM head nodes instead of specific nodes:
     - Set 'allheadnodes yes'
     - Clear the 'nodes' setting
  4. Optionally configure automatic 'scontrol takeover' on BCM failover:
     - Set partition[base]->failover->preFailoverScript to the takeover script
     - This ensures slurmctld moves to the active BCM head node during HA failover

After running this script:
  - slurmctld will run on the BCM head nodes
  - If takeover is enabled, Slurm will automatically failover with BCM

Prerequisites:
  - The slurm-server overlay should already exist (from cm-wlm-setup)
  - BCM HA should be configured if enabling automatic takeover
  - Run this AFTER migrating the slurmdbd (accounting database) if applicable

Options:
  --rollback          Rollback to original Slurm controller nodes
  --enable-takeover   Enable scontrol takeover on BCM failover (no prompts)
  --disable-takeover  Disable scontrol takeover on BCM failover
"""

import argparse
import os
import sys
import subprocess
import re
import time
from datetime import datetime


# Constants from BCM
SLURM_TAKEOVER_SCRIPT = '/cm/local/apps/cmd/scripts/slurm.takeover.sh'


def get_bcm_major_version() -> int:
    """Get the BCM major version number.
    
    Runs: cmsh -c "main; versioninfo" and parses the Cluster Manager version.
    
    Returns:
        Major version number (e.g., 10 or 11), or 10 as default if detection fails
    """
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    
    try:
        result = subprocess.run(
            [cmsh_path, '-c', 'main; versioninfo'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if 'cluster manager' in line.lower():
                    # Format: "Cluster Manager          10.0" or "Cluster Manager          11.0"
                    parts = line.split()
                    if len(parts) >= 3:
                        version_str = parts[-1]  # e.g., "10.0" or "11.0"
                        major_version = int(version_str.split('.')[0])
                        return major_version
        
        # Default to BCM 10 if detection fails
        return 10
        
    except Exception:
        # Default to BCM 10 if detection fails
        return 10


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
    
    This is important because some cmdaemon database updates only take effect
    on the active head node. If running on the passive node, the changes
    may be overwritten when cmha syncs the database.
    
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
        print(f"\n  Some configuration changes should be made on the active head node.")
        print(f"\n  Options:")
        print(f"    1) SSH to {active_node} and run this script there")
        print(f"    2) Run 'cmha makeactive' on this node first")
        print(f"    3) Continue anyway (some changes may not take effect)")
        
        answer = input("\n  Continue anyway? [y/N]: ").strip().lower()
        if answer not in ('y', 'yes'):
            print("Aborting. Please run on the active head node.")
            sys.exit(0)
        return False
    
    return True


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


def check_ha_available() -> bool:
    """Check if BCM HA is available and configured.
    
    Returns:
        True if HA is available, False otherwise
    """
    result = subprocess.run(
        ["cmha", "status"],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        return False
    
    # Check if we have two head nodes
    primary, secondary = get_bcm_headnodes()
    return primary is not None and secondary is not None


def find_slurm_wlm_cluster() -> str:
    """Find the name of the Slurm WLM cluster.
    
    Returns:
        Name of the Slurm WLM cluster (e.g., 'slurm')
        
    Raises:
        RuntimeError if no Slurm WLM cluster found
    """
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    
    result = subprocess.run(
        [cmsh_path, '-c', 'wlm; list'],
        capture_output=True,
        text=True,
        timeout=30
    )
    
    # Parse output to find Slurm cluster
    for line in result.stdout.split('\n'):
        line = line.strip()
        if not line or line.startswith('Name') or line.startswith('-'):
            continue
        # Look for "Slurm" in the Type column
        if 'slurm' in line.lower():
            parts = line.split()
            if parts:
                return parts[0]
    
    raise RuntimeError("Could not find a Slurm WLM cluster")


def get_wlm_primaryserver(cluster_name: str) -> str:
    """Get the current primaryserver setting from the WLM cluster.
    
    Args:
        cluster_name: Name of the WLM cluster
        
    Returns:
        Current primaryserver hostname or empty string if not set
    """
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    
    result = subprocess.run(
        [cmsh_path, '-c', f'wlm; use {cluster_name}; get primaryserver'],
        capture_output=True,
        text=True,
        timeout=30
    )
    
    # Parse output - look for a hostname (not a prompt or command echo)
    for line in result.stdout.split('\n'):
        line = line.strip()
        if not line:
            continue
        if line.startswith('['):
            continue
        # Should be a hostname
        if line and not line.startswith('primaryserver'):
            return line
    
    return ""


def update_wlm_primaryserver(cluster_name: str, primary_headnode: str, skip_confirm: bool = False) -> bool:
    """Update the WLM cluster's primaryserver to the active head node.
    
    According to BCM documentation:
    - If primaryserver is UNSET and slurm-server role is on head nodes, BCM automatically
      manages which head node is primary (config regenerated during failover)
    - If primaryserver is SET to a specific head node, that node is always primary
    
    This function sets primaryserver to the active head node for immediate operation,
    but also explains the auto-management option.
    
    Args:
        cluster_name: Name of the WLM cluster
        primary_headnode: Hostname of the active BCM head node
        skip_confirm: If True, don't prompt for confirmation
        
    Returns:
        True if configuration was updated successfully
    """
    print(f"\n{'=' * 65}")
    print("UPDATING WLM CLUSTER PRIMARY SERVER")
    print('=' * 65)
    
    current_primary = get_wlm_primaryserver(cluster_name)
    
    print(f"\nWLM Cluster: {cluster_name}")
    print(f"  Current primaryserver: {current_primary if current_primary else '(not set)'}")
    print(f"  Active BCM head node:  {primary_headnode}")
    
    if current_primary == primary_headnode:
        print(f"\n  ✓ primaryserver already set to active head node")
        return True
    
    # Explain the options
    print(f"\n  The primaryserver setting determines which head node runs the primary slurmctld.")
    print(f"  When set to a specific node, that node is always primary.")
    print(f"  When UNSET (cleared), BCM auto-manages: primary is always on active head node.")
    
    print(f"\nPlanned change:")
    print(f"  wlm[{cluster_name}]->primaryserver = {primary_headnode}")
    
    if not skip_confirm:
        if not confirm_prompt("\nApply this configuration? [Y/n]: ", default_yes=True):
            print("Skipping WLM primaryserver update.")
            return False
    
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    cmd = f"wlm; use {cluster_name}; set primaryserver {primary_headnode}; commit"
    
    try:
        result = subprocess.run(
            [cmsh_path, '-c', cmd],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"  ✗ Failed to set primaryserver: {result.stderr}")
            return False
        
        print(f"  ✓ Set primaryserver to {primary_headnode}")
        return True
        
    except Exception as e:
        print(f"  ✗ Error updating primaryserver: {e}")
        return False


def restart_slurmctld_services(skip_confirm: bool = False) -> bool:
    """Restart slurmctld services on nodes with slurmserver role.
    
    This forces regeneration of slurm.conf and restarts the controller.
    
    Args:
        skip_confirm: If True, don't prompt for confirmation
        
    Returns:
        True if services were restarted successfully
    """
    print(f"\n{'=' * 65}")
    print("RESTARTING SLURMCTLD SERVICES")
    print('=' * 65)
    
    if not skip_confirm:
        print(f"\n  This will restart slurmctld on all nodes with the slurmserver role.")
        print(f"  Running jobs will NOT be affected, but new job submissions may be")
        print(f"  briefly delayed during the restart.")
        
        if not confirm_prompt("\nRestart slurmctld services now? [Y/n]: ", default_yes=True):
            print("Skipping service restart. You will need to restart manually.")
            return False
    
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    
    print(f"\n  Restarting slurmctld services...")
    
    try:
        result = subprocess.run(
            [cmsh_path, '-c', 'device; foreach -l slurmserver (services; restart slurmctld)'],
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode != 0:
            print(f"  ⚠ Service restart may have encountered issues: {result.stderr}")
        else:
            print(f"  ✓ Sent restart command to slurmctld services")
        
        # Give services time to restart
        print(f"  Waiting for services to restart...")
        time.sleep(5)
        
        # Check status
        result = subprocess.run(
            [cmsh_path, '-c', 'device; foreach -l slurmserver (services; status slurmctld)'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        print(f"\n  Service status:")
        for line in result.stdout.split('\n'):
            line = line.strip()
            if line and ('slurmctld' in line.lower() or 'running' in line.lower() or 'stopped' in line.lower()):
                print(f"    {line}")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Error restarting services: {e}")
        return False


def find_slurmserver_overlay() -> tuple:
    """Find the configuration overlay that has the slurmserver role.
    
    Returns:
        Tuple of (overlay_name, current_nodes, allheadnodes_setting)
        
    Raises:
        RuntimeError if no overlay found with slurmserver role
    """
    # List all overlays and their roles
    cmsh_cmd = "configurationoverlay\nlist\nquit\n"
    result = run_cmsh(cmsh_cmd)
    
    # Parse output to find overlay with slurmserver role
    # Format: "Name (key)  Priority  All head nodes  Nodes  Categories  Roles"
    overlay_name = None
    
    for line in result.stdout.split('\n'):
        line = line.strip()
        if not line or line.startswith('Name') or line.startswith('-'):
            continue
        
        # Check if this line contains "slurmserver" in the Roles column
        if 'slurmserver' in line.lower():
            # First column is the overlay name
            parts = line.split()
            if parts:
                overlay_name = parts[0]
                break
    
    if not overlay_name:
        raise RuntimeError(
            "Could not find a configuration overlay with the slurmserver role. "
            "Please verify your BCM Slurm configuration."
        )
    
    # Get more details about this overlay
    cmsh_show = f"""configurationoverlay
use {overlay_name}
show
quit
"""
    result = run_cmsh(cmsh_show, check=False)
    
    current_nodes = ""
    allheadnodes = "no"
    
    for line in result.stdout.split('\n'):
        line_lower = line.lower().strip()
        
        # Parse the Nodes line
        if line_lower.startswith('nodes') and 'all head nodes' not in line_lower:
            parts = line.split()
            if len(parts) >= 2:
                # Join all parts after "Nodes" in case of comma-separated list
                current_nodes = ' '.join(parts[1:])
        
        # Parse the All head nodes line
        if 'all head nodes' in line_lower:
            parts = line.split()
            if parts:
                allheadnodes = parts[-1].lower()
    
    return (overlay_name, current_nodes, allheadnodes)


def get_current_prefailover_script() -> str:
    """Get the current preFailoverScript setting from partition failover.
    
    Returns:
        Current script path or empty string if not set
    """
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    
    # Use cmsh -c for cleaner output (no command echo)
    cmd = "partition; use base; failover; get prefailoverscript"
    
    try:
        result = subprocess.run(
            [cmsh_path, '-c', cmd],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        # The output should be just the value (or empty if not set)
        # Filter out any prompt lines and empty lines
        for line in result.stdout.split('\n'):
            line = line.strip()
            # Skip empty lines, prompt lines, and cmsh artifacts
            if not line:
                continue
            if line.startswith('['):
                continue
            # A valid script path should start with /
            if line.startswith('/'):
                return line
        
        return ""
        
    except Exception:
        return ""


def configure_scontrol_takeover(enable: bool, wlm_cluster: str = "slurm", 
                                 overlay_name: str = "slurm-server", skip_confirm: bool = False) -> bool:
    """Configure or remove scontrol takeover on BCM failover.
    
    When enabled, this does TWO things:
    1. Sets the preFailoverScript on the base partition to slurm.takeover.sh
    2. Enables takeover mode to prevent BCM from auto-restarting slurmctld:
       - BCM 10.x: wlm; set --extra takeover yes
       - BCM 11.x: configurationoverlay; roles; set slurmctldstartpolicy TAKEOVER
    
    The takeover mode setting prevents BCM from auto-restarting slurmctld
    when it stops due to a takeover, which would cause the takeover to fail.
    
    Args:
        enable: True to enable, False to disable
        wlm_cluster: Name of the WLM cluster (default: "slurm")
        overlay_name: Name of the slurm-server overlay (default: "slurm-server")
        skip_confirm: If True, don't prompt for confirmation
        
    Returns:
        True if configuration was updated successfully
    """
    print(f"\n{'=' * 65}")
    print("CONFIGURING SCONTROL TAKEOVER ON BCM FAILOVER")
    print('=' * 65)
    
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    
    # Detect BCM version
    bcm_version = get_bcm_major_version()
    print(f"\nDetected BCM version: {bcm_version}.x")
    
    # Check current settings
    current_script = get_current_prefailover_script()
    
    print(f"\nCurrent settings:")
    print(f"  preFailoverScript: {current_script if current_script else '(not set)'}")
    
    if enable:
        already_configured = current_script == SLURM_TAKEOVER_SCRIPT
        
        if current_script and current_script != SLURM_TAKEOVER_SCRIPT:
            print(f"\n  ⚠ WARNING: A different preFailoverScript is already set:")
            print(f"    Current: {current_script}")
            print(f"    Expected: {SLURM_TAKEOVER_SCRIPT}")
            print(f"\n  If you proceed, the existing script will be replaced.")
            print(f"  Alternatively, you can modify the existing script to call")
            print(f"  {SLURM_TAKEOVER_SCRIPT}")
            
            if not skip_confirm:
                if not confirm_prompt("\n  Replace existing preFailoverScript? [y/N]: ", default_yes=False):
                    print("  Skipping scontrol takeover configuration.")
                    return False
        
        print(f"\nPlanned changes:")
        print(f"  1. partition[base]->failover->preFailoverScript = {SLURM_TAKEOVER_SCRIPT}")
        if bcm_version >= 11:
            print(f"  2. configurationoverlay[{overlay_name}]->roles[slurmserver]->slurmctldstartpolicy = TAKEOVER")
        else:
            print(f"  2. wlm[{wlm_cluster}] --extra takeover = yes")
        print(f"\n  NOTE: The takeover mode setting is required for scontrol takeover")
        print(f"        to work. It prevents BCM from auto-restarting slurmctld after takeover.")
        
        if not skip_confirm:
            if not confirm_prompt("\nApply these configurations? [Y/n]: ", default_yes=True):
                print("Skipping scontrol takeover configuration.")
                return False
        
        success = True
        
        # Apply preFailoverScript
        if not already_configured:
            cmd = f"partition; use base; failover; set prefailoverscript {SLURM_TAKEOVER_SCRIPT}; commit"
            try:
                result = subprocess.run(
                    [cmsh_path, '-c', cmd],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode != 0:
                    print(f"  ✗ Failed to set preFailoverScript: {result.stderr}")
                    success = False
                else:
                    print(f"  ✓ Set preFailoverScript to {SLURM_TAKEOVER_SCRIPT}")
            except Exception as e:
                print(f"  ✗ Error setting preFailoverScript: {e}")
                success = False
        else:
            print(f"  ✓ preFailoverScript already configured")
        
        # Apply takeover mode setting based on BCM version
        if bcm_version >= 11:
            # BCM 11.x: Use slurmctldstartpolicy TAKEOVER on the slurmserver role
            cmd = f"configurationoverlay; use {overlay_name}; roles; use slurmserver; set slurmctldstartpolicy TAKEOVER; commit"
            setting_desc = f"slurmctldstartpolicy = TAKEOVER"
        else:
            # BCM 10.x: Use --extra takeover yes on WLM cluster
            cmd = f"wlm; use {wlm_cluster}; set --extra takeover yes; commit"
            setting_desc = f"wlm[{wlm_cluster}] --extra takeover = yes"
        
        try:
            result = subprocess.run(
                [cmsh_path, '-c', cmd],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                print(f"  ✗ Failed to set takeover mode: {result.stderr}")
                success = False
            else:
                print(f"  ✓ Set {setting_desc}")
        except Exception as e:
            print(f"  ✗ Error setting takeover mode: {e}")
            success = False
        
        return success
    
    else:
        # Disable takeover
        if not current_script:
            print(f"  ✓ preFailoverScript is already not set")
        elif current_script != SLURM_TAKEOVER_SCRIPT:
            print(f"\n  ⚠ Current preFailoverScript is not the Slurm takeover script:")
            print(f"    Current: {current_script}")
            print(f"  Not modifying preFailoverScript.")
        
        print(f"\nPlanned changes:")
        print(f"  1. partition[base]->failover->preFailoverScript = (cleared)")
        if bcm_version >= 11:
            print(f"  2. configurationoverlay[{overlay_name}]->roles[slurmserver]->slurmctldstartpolicy = ALWAYS")
        else:
            print(f"  2. wlm[{wlm_cluster}] --extra takeover = (cleared)")
        
        if not skip_confirm:
            if not confirm_prompt("\nApply these configurations? [Y/n]: ", default_yes=True):
                print("Skipping scontrol takeover removal.")
                return False
        
        success = True
        
        # Clear preFailoverScript
        if current_script == SLURM_TAKEOVER_SCRIPT:
            cmd = "partition; use base; failover; set prefailoverscript; commit"
            try:
                result = subprocess.run(
                    [cmsh_path, '-c', cmd],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode != 0:
                    print(f"  ✗ Failed to clear preFailoverScript: {result.stderr}")
                    success = False
                else:
                    print(f"  ✓ Cleared preFailoverScript")
            except Exception as e:
                print(f"  ✗ Error clearing preFailoverScript: {e}")
                success = False
        
        # Clear takeover mode setting based on BCM version
        if bcm_version >= 11:
            # BCM 11.x: Set slurmctldstartpolicy to ALWAYS
            cmd = f"configurationoverlay; use {overlay_name}; roles; use slurmserver; set slurmctldstartpolicy ALWAYS; commit"
            setting_desc = "slurmctldstartpolicy = ALWAYS"
        else:
            # BCM 10.x: Clear --extra takeover on WLM cluster
            cmd = f"wlm; use {wlm_cluster}; set --extra takeover no; commit"
            setting_desc = f"wlm[{wlm_cluster}] --extra takeover"
        
        try:
            result = subprocess.run(
                [cmsh_path, '-c', cmd],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                print(f"  ⚠ Could not clear takeover mode: {result.stderr}")
                # Don't fail on this - the setting might not exist
            else:
                print(f"  ✓ Cleared {setting_desc}")
        except Exception as e:
            print(f"  ⚠ Could not clear takeover mode: {e}")
        
        return success


def get_slurmctld_prolog_epilog(wlm_cluster: str) -> dict:
    """Get the PrologSlurmctld and EpilogSlurmctld settings from WLM.
    
    Args:
        wlm_cluster: Name of the WLM cluster
        
    Returns:
        Dictionary with 'prolog' and 'epilog' keys, values are paths or empty strings
    """
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    result = {'prolog': '', 'epilog': ''}
    
    for setting, key in [('prologslurmctld', 'prolog'), ('epilogslurmctld', 'epilog')]:
        try:
            cmd_result = subprocess.run(
                [cmsh_path, '-c', f'wlm; use {wlm_cluster}; get {setting}'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            # Parse output - look for a path (starts with /)
            for line in cmd_result.stdout.split('\n'):
                line = line.strip()
                if line and line.startswith('/'):
                    result[key] = line
                    break
                    
        except Exception:
            pass
    
    return result


def copy_slurmctld_scripts_to_headnodes(wlm_cluster: str, primary_headnode: str, 
                                         secondary_headnode: str, skip_confirm: bool = False) -> bool:
    """Check and copy PrologSlurmctld/EpilogSlurmctld scripts to BCM head nodes.
    
    If these scripts are configured and NOT on /cm/shared (shared storage),
    they need to be copied to the BCM head nodes for slurmctld to work.
    
    Args:
        wlm_cluster: Name of the WLM cluster
        primary_headnode: Primary BCM head node hostname
        secondary_headnode: Secondary BCM head node hostname (can be None)
        skip_confirm: If True, don't prompt for confirmation
        
    Returns:
        True if no issues or scripts were copied successfully
    """
    print(f"\n{'=' * 65}")
    print("CHECKING SLURMCTLD PROLOG/EPILOG SCRIPTS")
    print('=' * 65)
    
    scripts = get_slurmctld_prolog_epilog(wlm_cluster)
    
    print(f"\nCurrent WLM settings:")
    print(f"  PrologSlurmctld: {scripts['prolog'] if scripts['prolog'] else '(not set)'}")
    print(f"  EpilogSlurmctld: {scripts['epilog'] if scripts['epilog'] else '(not set)'}")
    
    # Check which scripts need to be copied
    scripts_to_copy = []
    
    for script_type, script_path in [('PrologSlurmctld', scripts['prolog']), 
                                      ('EpilogSlurmctld', scripts['epilog'])]:
        if not script_path:
            continue
        
        if script_path.startswith('/cm/shared'):
            print(f"\n  ✓ {script_type} is on shared storage: {script_path}")
            continue
        
        # Script is NOT on shared storage - needs to be copied
        scripts_to_copy.append((script_type, script_path))
        print(f"\n  ⚠ {script_type} is NOT on shared storage: {script_path}")
        print(f"    This script needs to be present on the BCM head nodes")
    
    if not scripts_to_copy:
        print(f"\n  ✓ No scripts need to be copied")
        return True
    
    # Determine target nodes
    target_nodes = [primary_headnode]
    if secondary_headnode:
        target_nodes.append(secondary_headnode)
    
    print(f"\nScripts to copy to BCM head nodes ({', '.join(target_nodes)}):")
    for script_type, script_path in scripts_to_copy:
        print(f"  - {script_path} ({script_type})")
    
    if not skip_confirm:
        if not confirm_prompt("\nCopy these scripts to the BCM head nodes? [Y/n]: ", default_yes=True):
            print("Skipping script copy. You will need to copy them manually.")
            print("\n  Manual copy commands:")
            for script_type, script_path in scripts_to_copy:
                for node in target_nodes:
                    print(f"    scp <source_node>:{script_path} {node}:{script_path}")
            return False
    
    # Copy the scripts
    success = True
    
    for script_type, script_path in scripts_to_copy:
        # First, find where the script currently exists
        # Try to get it from the current slurmctld nodes
        source_node = None
        
        # Check if it exists on any of the current slurmserver nodes
        cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
        result = subprocess.run(
            [cmsh_path, '-c', 'device; foreach -l slurmserver (get hostname)'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        current_slurmctl_nodes = [line.strip() for line in result.stdout.split('\n') if line.strip()]
        
        # Find a source node that has the script
        for node in current_slurmctl_nodes:
            if node in target_nodes:
                continue  # Skip if it's already a head node
            
            # Check if script exists on this node
            check_cmd = f'test -f "{script_path}" && echo exists'
            ssh_result = subprocess.run(
                ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ConnectTimeout=5',
                 node, check_cmd],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if ssh_result.returncode == 0 and 'exists' in ssh_result.stdout:
                source_node = node
                break
        
        if not source_node:
            print(f"\n  ⚠ Could not find source for {script_type}: {script_path}")
            print(f"    Please copy this script manually to the head nodes")
            success = False
            continue
        
        print(f"\n  Copying {script_type} from {source_node}...")
        
        # Copy to each target node
        for target_node in target_nodes:
            # Create target directory if needed
            target_dir = os.path.dirname(script_path)
            mkdir_result = subprocess.run(
                ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ConnectTimeout=5',
                 target_node, f'mkdir -p "{target_dir}"'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            # Use scp to copy via the source node
            # First copy to local temp, then to target
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = tmp.name
            
            try:
                # Copy from source to local temp
                scp_from = subprocess.run(
                    ['scp', '-o', 'StrictHostKeyChecking=no',
                     f'{source_node}:{script_path}', tmp_path],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if scp_from.returncode != 0:
                    print(f"    ✗ Failed to copy from {source_node}: {scp_from.stderr}")
                    success = False
                    continue
                
                # Copy from local temp to target
                scp_to = subprocess.run(
                    ['scp', '-o', 'StrictHostKeyChecking=no',
                     tmp_path, f'{target_node}:{script_path}'],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if scp_to.returncode != 0:
                    print(f"    ✗ Failed to copy to {target_node}: {scp_to.stderr}")
                    success = False
                    continue
                
                # Set execute permissions
                chmod_result = subprocess.run(
                    ['ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ConnectTimeout=5',
                     target_node, f'chmod +x "{script_path}"'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                print(f"    ✓ Copied to {target_node}:{script_path}")
                
            finally:
                # Clean up temp file
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
    
    return success


def update_slurmserver_overlay(skip_confirm: bool = False) -> tuple:
    """Update the slurm-server overlay to use all head nodes.
    
    This function:
    1. Finds the configuration overlay with slurmserver role
    2. Updates the overlay:
       - Sets 'allheadnodes' to yes
       - Clears the 'nodes' setting
    
    Args:
        skip_confirm: If True, don't prompt for confirmation
        
    Returns:
        Tuple of (success: bool, overlay_name: str, original_nodes: str)
    """
    print(f"\n{'=' * 65}")
    print("UPDATING SLURM-SERVER CONFIGURATION OVERLAY")
    print('=' * 65)
    
    # Find the overlay
    print("\nFinding configuration overlay with slurmserver role...")
    try:
        overlay_name, current_nodes, allheadnodes = find_slurmserver_overlay()
    except RuntimeError as e:
        print(f"  ✗ {e}")
        return (False, "", "")
    
    print(f"  Found overlay: {overlay_name}")
    
    # Show current configuration
    print(f"\nCurrent configuration:")
    print(f"  Overlay: {overlay_name}")
    print(f"    Nodes: {current_nodes if current_nodes else '(none)'}")
    print(f"    All head nodes: {allheadnodes}")
    
    # Check if already configured
    if allheadnodes == 'yes' and not current_nodes:
        print(f"\n  ✓ Overlay is already configured for all head nodes")
        return (True, overlay_name, current_nodes)
    
    # Show planned changes
    print(f"\nPlanned changes:")
    print(f"  Overlay: {overlay_name}")
    print(f"    nodes         : (will be cleared)")
    print(f"    allheadnodes  : yes")
    
    if not skip_confirm:
        if not confirm_prompt("\nApply these configuration changes? [Y/n]: ", default_yes=True):
            print("Skipping overlay configuration update.")
            return (False, overlay_name, current_nodes)
    
    # Build cmsh commands to update configuration
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    
    # Update overlay settings
    overlay_cmd = f"configurationoverlay; use {overlay_name}; set nodes; set allheadnodes yes; commit"
    
    print("\nApplying BCM configuration changes...")
    try:
        result = subprocess.run(
            [cmsh_path, '-c', overlay_cmd],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"Overlay update failed: {result.stderr}")
        
        print(f"  ✓ Updated overlay: allheadnodes=yes, nodes cleared")
        return (True, overlay_name, current_nodes)
        
    except Exception as e:
        print(f"  ✗ Failed to update BCM configuration: {e}")
        return (False, overlay_name, current_nodes)


def rollback_slurmserver_overlay(overlay_name: str, original_nodes: str, skip_confirm: bool = False) -> bool:
    """Rollback the slurm-server overlay to use specific nodes.
    
    Args:
        overlay_name: Name of the overlay to rollback
        original_nodes: Comma-separated list of original node names
        skip_confirm: If True, don't prompt for confirmation
        
    Returns:
        True if rollback was successful
    """
    print(f"\n{'=' * 65}")
    print("ROLLBACK SLURM-SERVER CONFIGURATION")
    print('=' * 65)
    
    if not original_nodes:
        print("\n  ✗ Error: No original nodes specified for rollback")
        print("  Use: --rollback --original-nodes node1,node2")
        return False
    
    print(f"\nPlanned changes:")
    print(f"  Overlay: {overlay_name}")
    print(f"    nodes         : {original_nodes}")
    print(f"    allheadnodes  : no")
    
    if not skip_confirm:
        if not confirm_prompt("\nApply these rollback changes? [Y/n]: ", default_yes=True):
            print("Skipping rollback.")
            return False
    
    cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
    
    # Update overlay settings
    overlay_cmd = f"configurationoverlay; use {overlay_name}; set allheadnodes no; set nodes {original_nodes}; commit"
    
    print("\nApplying rollback...")
    try:
        result = subprocess.run(
            [cmsh_path, '-c', overlay_cmd],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"Rollback failed: {result.stderr}")
        
        print(f"  ✓ Rollback complete: nodes={original_nodes}, allheadnodes=no")
        return True
        
    except Exception as e:
        print(f"  ✗ Failed to rollback: {e}")
        return False


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Migrate Slurm controller (slurmctld) from dedicated nodes to BCM head nodes.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run migration interactively (will prompt for scontrol takeover)
  %(prog)s

  # Migrate and enable scontrol takeover without prompts
  %(prog)s --enable-takeover

  # Migrate without enabling scontrol takeover
  %(prog)s --disable-takeover

  # Rollback to original Slurm controller nodes
  %(prog)s --rollback --original-nodes slurmctl-01,slurmctl-02

  # Just enable scontrol takeover (no overlay changes)
  %(prog)s --enable-takeover-only

  # Just disable scontrol takeover (no overlay changes)
  %(prog)s --disable-takeover-only
"""
    )
    
    parser.add_argument(
        '--rollback',
        action='store_true',
        help='Rollback migration to use original Slurm controller nodes'
    )
    
    parser.add_argument(
        '--original-nodes',
        type=str,
        metavar='NODES',
        help='Comma-separated list of original Slurm controller nodes (for --rollback)'
    )
    
    parser.add_argument(
        '--enable-takeover',
        action='store_true',
        help='Enable scontrol takeover on BCM failover (no prompts)'
    )
    
    parser.add_argument(
        '--disable-takeover',
        action='store_true',
        help='Disable scontrol takeover on BCM failover'
    )
    
    parser.add_argument(
        '--enable-takeover-only',
        action='store_true',
        help='Only configure scontrol takeover (skip overlay changes)'
    )
    
    parser.add_argument(
        '--disable-takeover-only',
        action='store_true',
        help='Only remove scontrol takeover (skip overlay changes)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.rollback and not args.original_nodes:
        parser.error("--rollback requires --original-nodes")
    
    if args.enable_takeover and args.disable_takeover:
        parser.error("Cannot use --enable-takeover and --disable-takeover together")
    
    if args.enable_takeover_only and args.disable_takeover_only:
        parser.error("Cannot use --enable-takeover-only and --disable-takeover-only together")
    
    return args


def main():
    args = parse_arguments()
    
    ensure_root()
    check_active_headnode()
    
    print("=" * 65)
    print("SLURM CONTROLLER MIGRATION TO BCM HEAD NODES")
    print("=" * 65)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Get BCM head node information
    primary_headnode, secondary_headnode = get_bcm_headnodes()
    ha_available = check_ha_available()
    bcm_version = get_bcm_major_version()
    
    print(f"BCM Information:")
    print(f"  BCM version          : {bcm_version}.x")
    print(f"  Primary head node    : {primary_headnode}")
    print(f"  Secondary head node  : {secondary_headnode if secondary_headnode else '(none)'}")
    print(f"  HA available         : {'yes' if ha_available else 'no'}")
    
    # Handle takeover-only modes
    if args.enable_takeover_only:
        if not ha_available:
            print("\n  ✗ Error: BCM HA is not available. Cannot configure scontrol takeover.")
            sys.exit(1)
        try:
            wlm_cluster = find_slurm_wlm_cluster()
        except RuntimeError as e:
            print(f"\n  ✗ {e}")
            sys.exit(1)
        configure_scontrol_takeover(enable=True, wlm_cluster=wlm_cluster, skip_confirm=True)
        print(f"\n{'=' * 65}")
        print("CONFIGURATION COMPLETE")
        print('=' * 65)
        sys.exit(0)
    
    if args.disable_takeover_only:
        try:
            wlm_cluster = find_slurm_wlm_cluster()
        except RuntimeError as e:
            print(f"\n  ✗ {e}")
            sys.exit(1)
        configure_scontrol_takeover(enable=False, wlm_cluster=wlm_cluster, skip_confirm=True)
        print(f"\n{'=' * 65}")
        print("CONFIGURATION COMPLETE")
        print('=' * 65)
        sys.exit(0)
    
    # Handle rollback mode
    if args.rollback:
        try:
            overlay_name, _, _ = find_slurmserver_overlay()
            wlm_cluster = find_slurm_wlm_cluster()
        except RuntimeError as e:
            print(f"\n  ✗ {e}")
            sys.exit(1)
        
        success = rollback_slurmserver_overlay(overlay_name, args.original_nodes)
        
        # Also update WLM primaryserver back to original node
        if success:
            original_primary = args.original_nodes.split(',')[0]  # First node is primary
            print(f"\n  Updating WLM primaryserver back to: {original_primary}")
            cmsh_path = "/cm/local/apps/cmd/bin/cmsh"
            result = subprocess.run(
                [cmsh_path, '-c', f'wlm; use {wlm_cluster}; set primaryserver {original_primary}; commit'],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                print(f"  ✓ Updated WLM primaryserver to {original_primary}")
            else:
                print(f"  ⚠ Could not update WLM primaryserver: {result.stderr}")
        
        # Also disable takeover if it was configured
        if success:
            current_script = get_current_prefailover_script()
            if current_script == SLURM_TAKEOVER_SCRIPT:
                print("\n  Also removing scontrol takeover configuration...")
                configure_scontrol_takeover(enable=False, wlm_cluster=wlm_cluster, 
                                           overlay_name=overlay_name, skip_confirm=True)
        
        # Restart services
        if success:
            restart_slurmctld_services(skip_confirm=True)
        
        print(f"\n{'=' * 65}")
        print("ROLLBACK SUMMARY")
        print('=' * 65)
        
        if success:
            print(f"\n✓ Slurm controller overlay rolled back to: {args.original_nodes}")
            print(f"\nVerification steps:")
            print(f"  1) Verify slurmctld is running on the original nodes:")
            print(f"       scontrol ping")
            print(f"  2) Check cluster status:")
            print(f"       sinfo")
        else:
            print(f"\n✗ Rollback failed. Please check the errors above.")
        
        print(f"\n{'=' * 65}")
        sys.exit(0 if success else 1)
    
    # Find WLM cluster name
    try:
        wlm_cluster = find_slurm_wlm_cluster()
    except RuntimeError as e:
        print(f"\n  ✗ {e}")
        sys.exit(1)
    
    current_primary = get_wlm_primaryserver(wlm_cluster)
    
    print(f"\nWLM Cluster Information:")
    print(f"  Cluster name         : {wlm_cluster}")
    print(f"  Current primaryserver: {current_primary if current_primary else '(not set)'}")
    
    # Normal migration flow
    print(
        "\nThis script will:\n"
        "  1) Check for PrologSlurmctld/EpilogSlurmctld scripts and copy if needed\n"
        "  2) Find the configuration overlay with the slurmserver role\n"
        "  3) Update it to run on all BCM head nodes (allheadnodes=yes)\n"
        "  4) Clear specific node assignments\n"
        f"  5) Update WLM primaryserver to active head node ({primary_headnode})\n"
        "  6) Restart slurmctld services to apply changes\n"
    )
    
    if ha_available:
        print(
            "  7) Optionally configure automatic 'scontrol takeover' on BCM failover\n"
            "     This ensures Slurm controller moves to the active head node\n"
        )
    
    answer = input("Proceed with migration? [y/N]: ").strip().lower()
    if answer not in ("y", "yes"):
        print("Aborting at user request.")
        sys.exit(0)
    
    # Step 0: Check and copy slurmctld prolog/epilog scripts if needed
    copy_slurmctld_scripts_to_headnodes(wlm_cluster, primary_headnode, secondary_headnode, skip_confirm=False)
    
    # Step 1: Update the overlay
    success, overlay_name, original_nodes = update_slurmserver_overlay(skip_confirm=False)
    
    if not success:
        print("\n✗ Migration failed. Configuration not changed.")
        sys.exit(1)
    
    # Step 2: Update WLM primaryserver
    wlm_updated = update_wlm_primaryserver(wlm_cluster, primary_headnode, skip_confirm=False)
    
    # Step 3: Configure scontrol takeover (if HA is available)
    takeover_configured = False
    
    if ha_available:
        if args.enable_takeover:
            takeover_configured = configure_scontrol_takeover(
                enable=True, wlm_cluster=wlm_cluster, overlay_name=overlay_name, skip_confirm=True)
        elif args.disable_takeover:
            configure_scontrol_takeover(
                enable=False, wlm_cluster=wlm_cluster, overlay_name=overlay_name, skip_confirm=True)
        else:
            # Prompt user
            print(f"\n{'=' * 65}")
            print("SCONTROL TAKEOVER CONFIGURATION")
            print('=' * 65)
            
            print(f"\nBCM HA is available. When the BCM head nodes failover, you can")
            print(f"optionally have Slurm automatically run 'scontrol takeover' to move")
            print(f"the primary slurmctld to the new active head node.")
            print(f"\nThis requires TWO settings (BCM version will be auto-detected):")
            print(f"  1. preFailoverScript = {SLURM_TAKEOVER_SCRIPT}")
            print(f"  2. Takeover mode (BCM 10: --extra takeover; BCM 11: slurmctldstartpolicy)")
            
            if confirm_prompt("\nWould you like to enable automatic scontrol takeover on BCM failover? [Y/n]: ", default_yes=True):
                takeover_configured = configure_scontrol_takeover(
                    enable=True, wlm_cluster=wlm_cluster, overlay_name=overlay_name, skip_confirm=True)
            else:
                print("Skipping scontrol takeover configuration.")
    
    # Step 4: Restart slurmctld services
    services_restarted = restart_slurmctld_services(skip_confirm=False)
    
    # Final summary
    print(f"\n{'=' * 65}")
    print("MIGRATION SUMMARY")
    print('=' * 65)
    
    print(f"\n✓ Slurm controller overlay '{overlay_name}' updated:")
    print(f"    allheadnodes: yes")
    print(f"    nodes: (cleared)")
    if original_nodes:
        print(f"    (Original nodes were: {original_nodes})")
    
    if wlm_updated:
        print(f"\n✓ WLM cluster '{wlm_cluster}' updated:")
        print(f"    primaryserver: {primary_headnode}")
    else:
        print(f"\n⚠ WLM primaryserver NOT updated")
        print(f"    Run manually: cmsh -c \"wlm; use {wlm_cluster}; set primaryserver {primary_headnode}; commit\"")
    
    if ha_available:
        if takeover_configured:
            print(f"\n✓ Scontrol takeover configured:")
            print(f"    partition[base]->failover->preFailoverScript = {SLURM_TAKEOVER_SCRIPT}")
        else:
            current_script = get_current_prefailover_script()
            if current_script == SLURM_TAKEOVER_SCRIPT:
                print(f"\n✓ Scontrol takeover already configured")
            else:
                print(f"\n⚠ Scontrol takeover NOT configured")
                print(f"    Run with --enable-takeover-only to configure later")
    
    if services_restarted:
        print(f"\n✓ slurmctld services restarted")
    else:
        print(f"\n⚠ slurmctld services NOT restarted")
        print(f"    Run manually: cmsh -c \"device; foreach -l slurmserver (services; restart slurmctld)\"")
    
    print(f"\nVerification steps:")
    print(f"  1) Check slurmctld is running:")
    print(f"       systemctl status slurmctld")
    print(f"       scontrol ping")
    print(f"  2) Verify cluster status:")
    print(f"       sinfo")
    print(f"       squeue")
    
    if ha_available and takeover_configured:
        print(f"\n  To test HA failover (CAUTION - this will cause a failover!):")
        print(f"       cmha manual-failover")
    
    if original_nodes:
        print(f"\n  To rollback if needed:")
        print(f"       {sys.argv[0]} --rollback --original-nodes {original_nodes}")
    
    print(f"\n{'=' * 65}")


if __name__ == "__main__":
    main()

