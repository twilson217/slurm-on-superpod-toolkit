# Slurm Upgrade Plan: 23.11 → 24.11 → 25.05

## Overview

This plan upgrades Slurm from version 23.11 to 25.05 using a two-step approach (23.11 → 24.11 → 25.05) as recommended by the admin manual to avoid full configuration wipes. The cluster uses Ubuntu 24.04, has 31 compute nodes, and uses Pyxis for containerized job support.

## Critical Outage Windows

**OUTAGE #1: First Upgrade (23.11 → 24.11)**
- **Duration**: ~2-3 hours
- **Breakdown**:
  - Pre-checks and job draining: 15-30 min (depends on running jobs)
  - Head nodes upgrade: 30-45 min
  - Compute nodes image update: 90-180 min (formula: 30-60 min per 10 nodes, ~2-4 batches for 31 nodes)
  - Pyxis reinstallation: 15-20 min
  - Validation: 10-15 min

**OUTAGE #2: Second Upgrade (24.11 → 25.05)**
- **Duration**: ~2-3 hours (same breakdown as above)

**TOTAL DOWNTIME**: 4-6 hours (can be split across maintenance windows)

## Image Deployment Time Formula

For compute nodes: **30-60 minutes per 10 nodes per head node** (max 10 concurrent PXE boots)
- 31 nodes = 4 batches (10+10+10+1) = ~2-3 hours total deployment time

---

## Phase 1: Pre-Upgrade Preparation (Both Head Nodes)

### Backup and Validation

1. **Document current state**:
   ```bash
   sinfo --version
   squeue -t R,CG  # Check running jobs
   sinfo -N -l     # Node status
   ```

2. **Backup Slurm configuration**:
   ```bash
   cp -r /etc/slurm /etc/slurm.backup.$(date +%Y%m%d)
   cp -r /cm/local/apps/slurm /cm/local/apps/slurm.backup.$(date +%Y%m%d)
   ```

3. **Identify software image path for compute nodes**:
   ```bash
   cmsh
   device; use <node-name>; show | grep "Software image"
   ```

4. **Verify BCM repository access**:
   ```bash
   apt update
   apt-cache search slurm24.11
   apt-cache search slurm25.05
   ```

---

## Phase 2: Upgrade to Slurm 24.11

### **START OUTAGE WINDOW #1**

### Step 1: Drain Nodes and Stop Jobs (15-30 min)

1. **Drain all compute nodes** (prevents new jobs):
   ```bash
   cmsh
   device
   drain -l slurmclient
   quit
   ```

2. **Wait for running jobs to complete** OR **cancel running jobs**:
   ```bash
   squeue -t R,CG  # Monitor running jobs
   # If canceling: scancel --state=RUNNING --user=<username>
   ```

3. **Verify no jobs are running**:
   ```bash
   squeue -t R,CG  # Should show empty
   ```

### Step 2: Stop Slurm Services on All Nodes (5 min)

1. **Stop services via cmsh**:
   ```bash
   cmsh
   device
   foreach -l slurmserver ( services; stop slurmctld )
   foreach -l slurmaccounting ( services; stop slurmdbd )
   foreach -l slurmclient ( services; stop slurmd )
   quit
   ```

2. **Verify services stopped on both head nodes**:
   ```bash
   # On headnode-01:
   systemctl status slurmctld.service
   systemctl status slurmdbd.service
   
   # On headnode-02:
   systemctl status slurmctld.service
   systemctl status slurmdbd.service
   ```

### Step 3: Remove Old Slurm 23.11 Packages on Active Head Node (10 min)

1. **List current packages**:
   ```bash
   apt list --installed | grep slurm
   ```

2. **Remove old Slurm packages**:
   ```bash
   apt remove slurm23.11*
   ```

3. **Verify removal**:
   ```bash
   apt list --installed | grep slurm  # Should show no slurm23.11
   ```

### Step 4: Install New Slurm 24.11 Packages on Active Head Node (10 min)

1. **Update package database**:
   ```bash
   apt update
   ```

2. **Install Slurm 24.11 packages**:
   ```bash
   apt install slurm24.11 slurm24.11-client slurm24.11-contribs \
     slurm24.11-devel slurm24.11-perlapi slurm24.11-slurmdbd \
     slurm24.11-slurmrestd
   ```

3. **Verify installation**:
   ```bash
   apt list --installed | grep slurm24.11
   ```

### Step 5: Update Slurm Version in BCM (5 min)

1. **Set new version in cmsh**:
   ```bash
   cmsh
   wlm use slurm
   set version 24.11
   commit
   quit
   ```

### Step 6: Restart Slurm Services on Active Head Node (5 min)

1. **Reload systemd and restart services**:
   ```bash
   systemctl daemon-reload
   systemctl restart slurmdbd
   systemctl restart slurmctld
   ```

2. **Check for systemd issues** (if slurmdbd fails to start):
   - Edit `/lib/systemd/system/slurmdbd.service`
   - Ensure `Type=simple`
   - Ensure ExecStart has `-D` option: `ExecStart=/cm/shared/apps/slurm/24.11.x/sbin/slurmdbd -D`
   - Run: `systemctl daemon-reload && systemctl restart slurmdbd && systemctl restart slurmctld`

3. **Verify services are active**:
   ```bash
   systemctl status slurmctld
   systemctl status slurmdbd
   ```

### Step 7: Repeat Steps 3-6 on Passive Head Node (15 min)

1. On headnode-02, remove slurm23.11 packages
2. Install slurm24.11 packages
3. Reload systemd and restart slurmctld only (NOT slurmdbd):
   ```bash
   systemctl daemon-reload
   systemctl restart slurmctld
   systemctl status slurmctld
   ```

### Step 8: Update Software Image for Compute Nodes (15 min)

1. **Enter chroot for compute node image**:
   ```bash
   cm-chroot-sw-img /cm/images/<your-software-image>/
   ```

2. **Update and remove old packages**:
   ```bash
   apt update
   apt list --installed | grep slurm
   apt remove slurm23.11*
   ```

3. **Install new client package**:
   ```bash
   apt install slurm24.11-client
   ```

4. **Verify and exit**:
   ```bash
   apt list --installed | grep slurm24.11
   exit
   ```

### Step 9: Deploy Updated Image to Compute Nodes (90-180 min)

1. **Update all compute nodes** (31 nodes, ~2-3 hours):
   ```bash
   cmsh
   device
   imageupdate -w -c <compute-category>
   quit
   ```

2. **Monitor deployment progress** (exit cmsh after seeing completion messages)

3. **Reload systemd and restart slurmd on all compute nodes**:
   ```bash
   pdsh -w <node-range> "systemctl daemon-reload && systemctl restart slurmd"
   ```

4. **Verify nodes are responding**:
   ```bash
   sinfo
   # All nodes should show as idle/allocated, not down
   ```

### Step 10: Reinstall Pyxis for Slurm 24.11 (15-20 min)

1. **Reinstall Pyxis** (compiles Pyxis for new Slurm version):
   ```bash
   cm-wlm-setup --reinstall-pyxis
   ```

2. **Verify Pyxis installation**:
   ```bash
   ls -la /cm/local/apps/slurm/current/lib/slurm/
   # Should see spank_pyxis.so for version 24.11
   ```

### Step 11: Start Slurm Services and Resume Nodes (5 min)

1. **Start services via cmsh**:
   ```bash
   cmsh
   device
   foreach -l slurmserver ( services; start slurmctld )
   foreach -l slurmaccounting ( services; start slurmdbd )
   foreach -l slurmclient ( services; start slurmd )
   quit
   ```

2. **Resume drained nodes**:
   ```bash
   cmsh
   device
   undrain -l slurmclient
   quit
   ```

3. **Verify cluster status**:
   ```bash
   sinfo --version  # Should show slurm 24.11.x
   sinfo -N -l      # All nodes should be idle
   ```

### Step 12: Validation Testing (10-15 min)

1. **Test basic job submission**:
   ```bash
   srun -N1 hostname
   ```

2. **Test Pyxis/Enroot functionality**:
   ```bash
   srun --container-image=ubuntu grep PRETTY /etc/os-release
   ```

3. **Check accounting database**:
   ```bash
   sacct -S now-1day
   ```

### **END OUTAGE WINDOW #1**

---

## Phase 3: Upgrade to Slurm 25.05

**Wait Period**: Optional - can proceed immediately or schedule for a later maintenance window

### **START OUTAGE WINDOW #2**

**Repeat Phase 2 Steps 1-12**, but replace all references to:
- Remove: `slurm24.11*` 
- Install: `slurm25.05` packages
- Set version: `25.05`
- Image path updates: ensure correct slurm25.05 paths

**Specific changes for 25.05 upgrade**:

- Step 3: `apt remove slurm24.11*`
- Step 4: `apt install slurm25.05 slurm25.05-client slurm25.05-contribs slurm25.05-devel slurm25.05-perlapi slurm25.05-slurmdbd slurm25.05-slurmrestd`
- Step 5: `set version 25.05`
- Step 8: In chroot - `apt remove slurm24.11* && apt install slurm25.05-client`
- Step 10: `cm-wlm-setup --reinstall-pyxis` (for Slurm 25.05)

### **END OUTAGE WINDOW #2**

---

## Phase 4: Post-Upgrade Validation

1. **Verify final version**:
   ```bash
   sinfo --version  # Should show slurm 25.05.x
   ```

2. **Run comprehensive tests**:
   - Single node job
   - Multi-node job
   - GPU job (if applicable)
   - Containerized job with Pyxis
   - Array jobs
   - Interactive jobs

3. **Monitor for issues**:
   ```bash
   tail -f /var/log/slurm/slurmctld.log
   tail -f /var/log/slurm/slurmdbd.log
   ```

4. **Check job history**:
   ```bash
   sacct -S now-1week
   ```

---

## Rollback Procedure

If issues occur during either upgrade phase:

1. **Stop Slurm services** (same as Step 2)

2. **Restore from backup**:
   ```bash
   apt remove slurm24.11* (or slurm25.05*)
   apt install slurm23.11 slurm23.11-client slurm23.11-contribs \
     slurm23.11-devel slurm23.11-perlapi slurm23.11-slurmdbd
   
   cmsh
   wlm use slurm
   set version 23.11
   commit
   quit
   ```

3. **Restore configuration** (if needed):
   ```bash
   cp -r /etc/slurm.backup.YYYYMMDD/* /etc/slurm/
   ```

4. **Revert software image** and redeploy to compute nodes

5. **Reinstall Pyxis for 23.11**

---

## Key Considerations

1. **Jobs must not be running during upgrade** - the admin manual strongly recommends this
2. **Test in lab environment first** - validate impact of upgrades before production
3. **Backup everything** - configurations, images, database
4. **Both head nodes must be upgraded** - do not leave versions mismatched
5. **All Slurm packages must match versions** - across all nodes
6. **Pyxis must be reinstalled after each major version upgrade**
7. **Communication** - notify users well in advance of maintenance windows
8. **systemd service files may need adjustment** - particularly slurmdbd.service

## Time Estimates Summary

| Task | Duration |
|------|----------|
| Pre-upgrade preparation | 30-45 min |
| Upgrade to 24.11 (full outage) | 2-3 hours |
| Upgrade to 25.05 (full outage) | 2-3 hours |
| Post-upgrade validation | 30 min |
| **TOTAL** | **5-7 hours** |

**Can be split**: The two upgrades can occur in separate maintenance windows if desired.

