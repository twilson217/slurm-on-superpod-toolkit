# Slurm Upgrade - Quick Reference Card

## Current Status
- **Current Version**: Slurm 23.11.11 (detected on this system)
- **Target Version**: Slurm 25.05
- **Upgrade Path**: 23.11 → 24.11 → 25.05 (two-step)
- **Platform**: Ubuntu 24.04, BCM-managed
- **Nodes**: 31 compute nodes

---

## Essential Commands

### Healthcheck Tool

```bash
# Basic healthcheck
./slurm-healthcheck.py

# Verbose healthcheck
./slurm-healthcheck.py -v

# Pre-upgrade baseline capture
./slurm-healthcheck.py --pre-upgrade -o baseline-23.11-$(date +%Y%m%d).json

# Post-upgrade validation
./slurm-healthcheck.py --post-upgrade --baseline baseline-23.11-YYYYMMDD.json -v

# JSON output for automation
./slurm-healthcheck.py --json -o results.json
```

### Upgrade Commands (First Phase: 23.11 → 24.11)

```bash
# 1. Drain nodes
cmsh
device; drain -l slurmclient; quit

# 2. Stop services
cmsh
device
foreach -l slurmserver ( services; stop slurmctld )
foreach -l slurmaccounting ( services; stop slurmdbd )
foreach -l slurmclient ( services; stop slurmd )
quit

# 3. Remove old packages
apt remove slurm23.11*

# 4. Install new packages
apt install slurm24.11 slurm24.11-client slurm24.11-contribs \
  slurm24.11-devel slurm24.11-perlapi slurm24.11-slurmdbd \
  slurm24.11-slurmrestd

# 5. Update BCM version
cmsh
wlm use slurm; set version 24.11; commit; quit

# 6. Restart services
systemctl daemon-reload
systemctl restart slurmdbd
systemctl restart slurmctld

# 7. Update compute node image
cm-chroot-sw-img /cm/images/<your-image>/
apt update && apt remove slurm23.11* && apt install slurm24.11-client
exit

# 8. Deploy image
cmsh
device; imageupdate -w -c <compute-category>; quit

# 9. Restart slurmd on nodes
pdsh -w <node-range> "systemctl daemon-reload && systemctl restart slurmd"

# 10. Reinstall Pyxis
cm-wlm-setup --reinstall-pyxis

# 11. Resume nodes
cmsh
device; undrain -l slurmclient; quit
```

### Upgrade Commands (Second Phase: 24.11 → 25.05)

Repeat above commands, but replace:
- `slurm24.11` with `slurm25.05`
- `version 24.11` with `version 25.05`

---

## Time Estimates

| Task | Duration |
|------|----------|
| Pre-upgrade prep & baseline | 30-45 min |
| Head node package upgrade | 30-45 min |
| Compute node image deployment | 2-3 hours (31 nodes) |
| Pyxis reinstallation | 15-20 min |
| Validation & testing | 10-15 min |
| **Per upgrade phase** | **2-3 hours** |
| **Total (both phases)** | **4-6 hours** |

**Image deployment formula**: 30-60 min per 10 nodes (max 10 concurrent PXE boots)

---

## Critical Checklist

### Before Upgrade
- [ ] Run healthcheck: `./slurm-healthcheck.py -v`
- [ ] Capture baseline: `./slurm-healthcheck.py --pre-upgrade -o baseline.json`
- [ ] Backup baseline file to safe location
- [ ] Backup Slurm configs: `cp -r /etc/slurm /etc/slurm.backup.$(date +%Y%m%d)`
- [ ] Notify users of maintenance window
- [ ] Verify BCM repository access: `apt-cache search slurm24.11 slurm25.05`
- [ ] Identify software image path for compute nodes

### During Upgrade
- [ ] Drain nodes and wait for jobs to complete
- [ ] Stop all Slurm services (controller, db, clients)
- [ ] Remove old packages on both head nodes
- [ ] Install new packages on both head nodes
- [ ] Update BCM version setting
- [ ] Check for systemd service file issues (Type=simple, -D flag)
- [ ] Update software image in chroot
- [ ] Deploy image to all compute nodes
- [ ] Restart services in correct order (slurmdbd → slurmctld → slurmd)
- [ ] Reinstall Pyxis with cm-wlm-setup
- [ ] Resume drained nodes

### After Upgrade
- [ ] Run post-upgrade validation: `./slurm-healthcheck.py --post-upgrade -b baseline.json -v`
- [ ] Verify version: `sinfo --version`
- [ ] Check all nodes: `sinfo -N -l`
- [ ] Test job submission: `srun -N1 hostname`
- [ ] Test Pyxis: `srun --container-image=ubuntu grep PRETTY /etc/os-release`
- [ ] Check accounting: `sacct -S now-1day`
- [ ] Monitor logs for errors
- [ ] Capture new baseline before next upgrade

---

## Accounting Data Protection

The healthcheck validates NO LOSS of:
- ✓ Users (sacctmgr show user)
- ✓ Accounts (sacctmgr show account)
- ✓ QOS settings (sacctmgr show qos)
- ✓ Associations (sacctmgr show associations)
- ✓ TRES configuration (sacctmgr show tres)
- ✓ Job history (sacct)
- ✓ Partitions
- ✓ Node configurations

---

## Troubleshooting Quick Fixes

### Service Won't Start
```bash
# Check systemd unit file
vi /lib/systemd/system/slurmdbd.service
# Ensure: Type=simple
# Ensure: ExecStart has -D flag
systemctl daemon-reload
systemctl restart slurmdbd slurmctld
```

### Nodes Not Responding
```bash
# Check node status
sinfo -N -l

# Restart slurmd on problem nodes
pdsh -w <nodes> "systemctl restart slurmd"

# Check logs
ssh <node> "tail -f /var/log/slurm/slurmd.log"
```

### Image Deployment Slow
```bash
# Monitor progress
cmsh
device; show | grep -i provision
```

### Accounting Database Issues
```bash
# Check slurmdbd connection
sacctmgr show cluster

# Check database daemon
systemctl status slurmdbd
tail -f /var/log/slurm/slurmdbd.log
```

---

## File Locations

### Documentation
- Upgrade plan: `slurm-upgrade-23.11-to-25.05-plan.md`
- Healthcheck guide: `README-healthcheck.md`
- This quick ref: `QUICK-REFERENCE.md`

### Tools
- Healthcheck script: `./slurm-healthcheck.py`
- Configuration: `healthcheck-config.conf`

### Logs
- Controller: `/var/log/slurm/slurmctld.log`
- Database: `/var/log/slurm/slurmdbd.log`
- Compute nodes: `/var/log/slurm/slurmd.log`

### Configs
- Main config: `/etc/slurm/slurm.conf`
- Database config: `/etc/slurm/slurmdbd.conf`
- Systemd units: `/lib/systemd/system/slurm*.service`

---

## Exit Codes

Healthcheck script returns:
- **0** = Healthy (all checks passed)
- **1** = Degraded (warnings present)
- **2** = Critical (failures present)

Use in scripts:
```bash
./slurm-healthcheck.py
if [ $? -ne 0 ]; then
    echo "Issues detected, review before proceeding"
    exit 1
fi
```

---

## Important Reminders

⚠️ **Jobs must not be running during upgrade**

⚠️ **Test in lab environment first**

⚠️ **Both head nodes must be upgraded together**

⚠️ **Pyxis must be reinstalled after each major version change**

⚠️ **All package versions must match across all nodes**

⚠️ **Keep baseline files safe - needed for validation**

---

## Emergency Contacts

- Admin manual: `.docs/admin-manual.txt`
- CBU docs: `.docs/CBU_Upgrade.txt`
- Support: [Your support contact]

---

**Last Updated**: 2025-11-19
**Environment**: BCM-managed cluster, Ubuntu 24.04, 31 nodes

