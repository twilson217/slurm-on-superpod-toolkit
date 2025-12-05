# Migrate Slurm Controller (slurmctld) to BCM Head Nodes

This document provides step-by-step instructions for manually migrating the Slurm controller (slurmctld) from dedicated Slurm controller nodes to BCM head nodes. After migration, slurmctld will run on the BCM head nodes with optional automatic failover integration.

> **Note:** The `migrate-slurmctl-to-bcm.py` script automates this entire process. Use this manual procedure only if you need to understand the steps or troubleshoot issues.
>
> **Script Options:**
> - `--enable-takeover` - Enable scontrol takeover on BCM failover (no prompts)
> - `--disable-takeover` - Disable scontrol takeover
> - `--enable-takeover-only` - Only configure takeover (skip overlay changes)
> - `--disable-takeover-only` - Only remove takeover configuration
> - `--rollback --original-nodes <nodes>` - Rollback migration to original nodes

## Prerequisites

- Run all commands on the **active BCM head node** as root
- `cmsh` available at `/cm/local/apps/cmd/bin/cmsh`
- BCM HA configured (if enabling automatic scontrol takeover)
- The slurm-server overlay should already exist (from cm-wlm-setup)

## Overview

1. Identify current slurm-server configuration
2. Update the slurm-server overlay to use all head nodes
3. Update WLM primaryserver to active head node
4. (Optional) Configure automatic scontrol takeover on BCM failover
5. Restart slurmctld services
6. Verify operation

---

## Step 1: Identify Current Slurm-Server Configuration

### Find the overlay with slurmserver role

```bash
cmsh -c "configurationoverlay; list" | grep -i slurm
```

Look for the overlay that has `slurmserver` in the Roles column. Common names are `slurm-server` or `slurm-controller`.

### View current overlay settings

```bash
OVERLAY_NAME="slurm-server"  # Replace with your overlay name
cmsh -c "configurationoverlay; use ${OVERLAY_NAME}; show"
```

Note the current values for:
- `Nodes` - Which nodes currently run slurmctld
- `All head nodes` - Whether it's set to run on all head nodes

### Identify BCM head nodes

```bash
cmha status
```

Note which node has `*` (active) and which is passive.

---

## Step 2: Update the Slurm-Server Overlay

### Set overlay to run on all head nodes

```bash
OVERLAY_NAME="slurm-server"  # Replace with your overlay name

# Clear specific node assignments and enable all head nodes
cmsh -c "configurationoverlay; use ${OVERLAY_NAME}; set nodes; set allheadnodes yes; commit"
```

### Verify the change

```bash
cmsh -c "configurationoverlay; use ${OVERLAY_NAME}; show" | grep -E "(Nodes|All head)"
```

Expected output:
```
Nodes                                   
All head nodes                          yes
```

---

## Step 3: Update WLM Primary Server

The WLM cluster has a `primaryserver` setting that determines which node runs the primary slurmctld. This must be updated to point to the active BCM head node.

### Check current primaryserver

```bash
cmsh -c "wlm; use slurm; get primaryserver"
```

If it shows the old controller node (e.g., `slurmctl-01`), it needs to be updated.

### Update primaryserver to active head node

```bash
ACTIVE_HEAD="travisw-c1-a"  # Replace with your active BCM head node
cmsh -c "wlm; use slurm; set primaryserver ${ACTIVE_HEAD}; commit"
```

### Verify the change

```bash
cmsh -c "wlm; use slurm; get primaryserver"
```

### Alternative: Let BCM auto-manage primaryserver

According to BCM documentation, if `primaryserver` is **unset** and the slurm-server role is on head nodes:
- slurmctld starts on both head nodes
- During failover, the Slurm configuration is regenerated
- The primary slurmctld is always on the active head node

To enable auto-management (clear primaryserver):
```bash
cmsh -c "wlm; use slurm; clear primaryserver; commit"
```

> **Note:** For immediate operation after migration, setting `primaryserver` explicitly is recommended. The auto-management option is best for ongoing HA operations.

---

## Step 4: (Optional) Configure Automatic Scontrol Takeover

When BCM HA fails over from one head node to another, Slurm can automatically run `scontrol takeover` to move the primary slurmctld to the new active head node. 

**This requires TWO settings:**
1. `preFailoverScript` - tells BCM to run the takeover script during failover
2. A "takeover mode" setting that prevents BCM from auto-restarting slurmctld after takeover

> **Important:** Without the takeover mode setting, BCM will automatically restart slurmctld after it stops, causing the takeover to fail or behave unexpectedly.

### BCM Version Differences

The takeover mode setting differs between BCM versions:

| BCM Version | Takeover Mode Setting |
|-------------|----------------------|
| **BCM 10.x** | `cmsh -c "wlm; use slurm; set --extra takeover yes; commit"` |
| **BCM 11.x** | `cmsh -c "configurationoverlay; use slurm-server; roles; use slurmserver; set slurmctldstartpolicy TAKEOVER; commit"` |

To check your BCM version:
```bash
cmsh -c "main; versioninfo" | grep -i "cluster manager"
```

### Check current settings

```bash
# Check preFailoverScript
cmsh -c "partition; use base; failover; get prefailoverscript"

# Check takeover mode (BCM 10.x)
cmsh -c "wlm; use slurm; show" | grep -i extra

# Check takeover mode (BCM 11.x)
cmsh -c "configurationoverlay; use slurm-server; roles; use slurmserver; get slurmctldstartpolicy"
```

### Enable automatic scontrol takeover

**Both settings are required:**

```bash
# 1. Set the preFailoverScript (same for BCM 10 and 11)
cmsh -c "partition; use base; failover; set prefailoverscript /cm/local/apps/cmd/scripts/slurm.takeover.sh; commit"

# 2. Enable takeover mode (BCM 10.x)
cmsh -c "wlm; use slurm; set --extra takeover yes; commit"

# 2. Enable takeover mode (BCM 11.x) - use this instead for BCM 11
# cmsh -c "configurationoverlay; use slurm-server; roles; use slurmserver; set slurmctldstartpolicy TAKEOVER; commit"
```

### Verify the settings

```bash
cmsh -c "partition; use base; failover; get prefailoverscript"
```

Expected output:
```
/cm/local/apps/cmd/scripts/slurm.takeover.sh
```

### Understanding the takeover script

The `/cm/local/apps/cmd/scripts/slurm.takeover.sh` script is provided by BCM and does the following:

1. Is called by CMDaemon when a failover occurs, with argument `ACTIVE` on the new active node
2. Checks if slurmctld is running
3. Determines which Slurm cluster configuration applies
4. If this node is the backup slurmctld, runs `scontrol takeover`
5. This promotes the secondary slurmctld to primary

### If you have an existing preFailoverScript

If there's already a preFailoverScript configured for other purposes:

1. Note the existing script path
2. Modify that script to also call the Slurm takeover script:
   
   ```bash
   # Add this to your existing preFailoverScript
   /cm/local/apps/cmd/scripts/slurm.takeover.sh "$@"
   ```

---

## Step 5: Restart Slurmctld Services

### Stop slurmctld on old nodes (if still running)

If slurmctld was running on dedicated controller nodes:

```bash
# SSH to each old controller and stop the service
ssh slurmctl-01 'systemctl stop slurmctld'
ssh slurmctl-02 'systemctl stop slurmctld'
```

### Start/restart slurmctld on head nodes

```bash
cmsh -c "device; foreach -l slurmserver (services; restart slurmctld)"
```

Or individually:

```bash
systemctl restart slurmctld
```

---

## Step 6: Verify Operation

### Check slurmctld is running

```bash
systemctl status slurmctld
```

### Check Slurm controller status

```bash
scontrol ping
```

Expected output shows which controller is primary:
```
Slurmctld(primary) at head1 is UP
Slurmctld(backup) at head2 is UP
```

### Verify cluster status

```bash
sinfo
squeue
```

### Test job submission

```bash
srun hostname
```

---

## Disabling Automatic Scontrol Takeover

If you need to disable the automatic scontrol takeover:

```bash
# Clear the preFailoverScript
cmsh -c "partition; use base; failover; set prefailoverscript; commit"

# Disable takeover mode (BCM 10.x)
cmsh -c "wlm; use slurm; set --extra takeover no; commit"

# Disable takeover mode (BCM 11.x) - use this instead for BCM 11
# cmsh -c "configurationoverlay; use slurm-server; roles; use slurmserver; set slurmctldstartpolicy ALWAYS; commit"
```

This clears both settings required for automatic takeover.

---

## Rollback Procedure

If you need to revert the slurm-server overlay to use specific nodes:

### Using the script (recommended)

```bash
./migrate-slurmctl-to-bcm.py --rollback --original-nodes slurmctl-01,slurmctl-02
```

### Manual rollback

1. Update the overlay to use specific nodes:
   ```bash
   OVERLAY_NAME="slurm-server"
   ORIGINAL_NODES="slurmctl-01,slurmctl-02"  # Replace with your original nodes
   
   cmsh -c "configurationoverlay; use ${OVERLAY_NAME}; set allheadnodes no; set nodes ${ORIGINAL_NODES}; commit"
   ```

2. Update WLM primaryserver back to original node:
   ```bash
   ORIGINAL_PRIMARY="slurmctl-01"  # Replace with your original primary
   cmsh -c "wlm; use slurm; set primaryserver ${ORIGINAL_PRIMARY}; commit"
   ```

3. (Optional) Remove scontrol takeover if configured:
   ```bash
   cmsh -c "partition; use base; failover; set prefailoverscript; commit"
   ```

4. Restart slurmctld on the original nodes:
   ```bash
   cmsh -c "device; foreach -l slurmserver (services; restart slurmctld)"
   ```

5. Verify:
   ```bash
   scontrol ping
   sinfo
   ```

---

## Troubleshooting

### slurmctld Fails to Start on Head Nodes

Check the slurmctld logs:
```bash
tail -100 /var/log/slurmctld.log
journalctl -u slurmctld -n 100
```

Common issues:
- **Port already in use**: Another slurmctld instance may be running
- **Configuration mismatch**: Check `/cm/shared/apps/slurm/var/etc/slurm/slurm.conf`

### Scontrol Takeover Not Working During Failover

1. **Verify the takeover mode setting is enabled** (most common issue):

   For BCM 10.x:
   ```bash
   cmsh -c "wlm; use slurm; show" | grep -i extra
   ```
   If not set, enable it:
   ```bash
   cmsh -c "wlm; use slurm; set --extra takeover yes; commit"
   ```

   For BCM 11.x:
   ```bash
   cmsh -c "configurationoverlay; use slurm-server; roles; use slurmserver; get slurmctldstartpolicy"
   ```
   If not set to TAKEOVER, enable it:
   ```bash
   cmsh -c "configurationoverlay; use slurm-server; roles; use slurmserver; set slurmctldstartpolicy TAKEOVER; commit"
   ```

   > **Important:** Without this setting, BCM auto-restarts slurmctld after takeover, causing it to fail.

2. Verify the preFailoverScript is set:
   ```bash
   cmsh -c "partition; use base; failover; get prefailoverscript"
   ```

3. Check if the takeover script exists:
   ```bash
   ls -la /cm/local/apps/cmd/scripts/slurm.takeover.sh
   ```

4. Check the takeover log after a failover:
   ```bash
   cat /var/log/slurmtakeover.sh.log
   ```

5. Enable debug mode in the takeover script by running:
   ```bash
   /cm/local/apps/cmd/sbin/cmdaemonctl set full-status
   ```
   This enables logging to `/var/log/slurmtakeover.sh.log`

### Jobs Fail After Migration

1. Verify slurmctld is accessible from compute nodes:
   ```bash
   # From a compute node:
   scontrol ping
   ```

2. Check network connectivity to the BCM head nodes
3. Verify the compute nodes have the correct `SlurmctldHost` in their slurm.conf

### BCM Failover Causes Slurm Issues

If automatic takeover is not configured and BCM fails over:

1. slurmctld may still be running on the now-passive head node
2. Jobs may queue waiting for the primary controller

To manually trigger takeover after BCM failover:
```bash
# On the new active (now backup slurmctld) head node:
scontrol takeover
```

---

## Configuration Reference

### Key cmsh Commands

| Action | Command |
|--------|---------|
| Check BCM version | `cmsh -c "main; versioninfo" \| grep "Cluster Manager"` |
| List overlays | `cmsh -c "configurationoverlay; list"` |
| Show overlay details | `cmsh -c "configurationoverlay; use OVERLAY; show"` |
| Set all head nodes | `cmsh -c "configurationoverlay; use OVERLAY; set allheadnodes yes; commit"` |
| Clear nodes | `cmsh -c "configurationoverlay; use OVERLAY; set nodes; commit"` |
| Get WLM primaryserver | `cmsh -c "wlm; use slurm; get primaryserver"` |
| Set WLM primaryserver | `cmsh -c "wlm; use slurm; set primaryserver HOSTNAME; commit"` |
| Clear WLM primaryserver | `cmsh -c "wlm; use slurm; clear primaryserver; commit"` |
| Enable takeover (BCM 10) | `cmsh -c "wlm; use slurm; set --extra takeover yes; commit"` |
| Disable takeover (BCM 10) | `cmsh -c "wlm; use slurm; set --extra takeover no; commit"` |
| Enable takeover (BCM 11) | `cmsh -c "configurationoverlay; use slurm-server; roles; use slurmserver; set slurmctldstartpolicy TAKEOVER; commit"` |
| Disable takeover (BCM 11) | `cmsh -c "configurationoverlay; use slurm-server; roles; use slurmserver; set slurmctldstartpolicy ALWAYS; commit"` |
| Set preFailoverScript | `cmsh -c "partition; use base; failover; set prefailoverscript PATH; commit"` |
| Clear preFailoverScript | `cmsh -c "partition; use base; failover; set prefailoverscript; commit"` |
| Restart slurmctld | `cmsh -c "device; foreach -l slurmserver (services; restart slurmctld)"` |

### Important Paths

| Path | Description |
|------|-------------|
| `/cm/local/apps/cmd/scripts/slurm.takeover.sh` | BCM's scontrol takeover script |
| `/var/log/slurmtakeover.sh.log` | Takeover script log (when debug enabled) |
| `/cm/shared/apps/slurm/var/etc/slurm/slurm.conf` | Main Slurm configuration |
| `/var/log/slurmctld.log` | Slurm controller log |

---

## Post-Migration Checklist

- [ ] slurm-server overlay set to `allheadnodes yes`
- [ ] slurmctld running on both BCM head nodes
- [ ] `scontrol ping` shows both controllers UP
- [ ] (Optional) preFailoverScript configured for automatic takeover
- [ ] `sinfo` shows nodes in expected states
- [ ] Job submission works: `srun hostname`
- [ ] (If applicable) Old controller nodes stopped/decommissioned

