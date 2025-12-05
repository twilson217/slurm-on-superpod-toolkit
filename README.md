# Slurm on SuperPOD Toolkit

Tools for managing Slurm upgrades, healthchecks, backups, and migrations in BCM-managed DGX SuperPOD clusters.

> **Note:** This toolkit was developed and tested against **BCM 10.x**. Some commands differ in BCM 11.x (e.g., the takeover mode setting). The scripts attempt to detect the BCM version and use the appropriate commands, but please verify compatibility if running on BCM 11.x.

## Scripts

### `healthcheck-slurm.py`

Comprehensive health validation for Slurm clusters. Run before/after upgrades to verify cluster state.

```bash
./healthcheck-slurm.py                    # Full healthcheck
./healthcheck-slurm.py --maint            # Skip job submission tests (maintenance mode)
./healthcheck-slurm.py --pre-upgrade      # Capture baseline before upgrade
./healthcheck-slurm.py --post-upgrade     # Compare against baseline after upgrade
```

**Features:** Service status, node health, HA validation (including BCM cmha MySQL HA), partition checks, Pyxis/Enroot, GPU/GRES, log analysis, database backup validation.

**Accounting Discovery:** Automatically discovers accounting nodes even when using `allheadnodes yes` overlay configuration. Uses `cmha status` for BCM head node MySQL HA validation.

**Config:** `healthcheck-config.conf`

---

### `backup-slurm-db.py`

Backup and restore the Slurm accounting database. Reads connection details from `slurmdbd.conf`.

```bash
./backup-slurm-db.py                          # Backup to default location
./backup-slurm-db.py -o /path/to/dir          # Backup to specific directory
./backup-slurm-db.py --retention 7            # Keep only last 7 days
./backup-slurm-db.py --restore backup.sql.gz  # Restore from backup
./backup-slurm-db.py --restore backup.sql -y  # Restore without confirmation
./backup-slurm-db.py --verify-only FILE       # Verify backup integrity
```

**For cron:** Uncomment `SLURM_DB_DEFAULT_BACKUP_DIR` at top of script to set default backup location.

---

### `migrate-slurmdb-to-bcm.py`

Migrate Slurm accounting database from dedicated controllers to BCM head nodes. Automatically updates BCM configuration via cmsh.

```bash
./migrate-slurmdb-to-bcm.py                    # Full migration
./migrate-slurmdb-to-bcm.py --reupdate-primary # Re-run cmdaemon DB update only
./migrate-slurmdb-to-bcm.py --rollback \       # Rollback to original controllers
    --original-primary slurmctl-01 \
    --original-backup slurmctl-02
```

**What it does:**
1. Dumps database from current `StorageHost`
2. Imports into local MariaDB on BCM head node
3. Updates `slurmaccounting` role via cmsh:
   - `primary` → active BCM head node hostname
   - `storagehost` → `master` (BCM HA virtual hostname)
4. Updates overlay to use `allheadnodes yes`
5. Creates systemd drop-in file on both head nodes (clears `ConditionPathExists` check)

**Options:**
- `--reupdate-primary` — Re-run only the cmdaemon database update for the slurmaccounting primary. Useful if the primary field wasn't properly updated during initial migration.
- `--rollback` — Revert BCM configuration to use original Slurm controllers. Requires `--original-primary` and optionally `--original-backup`.

**Manual procedure:** See `docs/migrate-slurmdb-to-bcm.md` for step-by-step manual instructions.

---

### `migrate-slurmctl-to-bcm.py`

Migrate Slurm controller (slurmctld) from dedicated nodes to BCM head nodes. Configures automatic failover integration.

```bash
./migrate-slurmctl-to-bcm.py                    # Full migration (interactive)
./migrate-slurmctl-to-bcm.py --enable-takeover  # Enable scontrol takeover (no prompts)
./migrate-slurmctl-to-bcm.py --enable-takeover-only  # Only configure takeover
./migrate-slurmctl-to-bcm.py --rollback \       # Rollback to original controllers
    --original-nodes slurmctl-01,slurmctl-02
```

**What it does:**
1. Updates slurm-server overlay to use `allheadnodes yes`
2. Clears specific node assignments
3. Updates WLM `primaryserver` to active head node
4. (Optional) Configures automatic `scontrol takeover` on BCM failover:
   - Sets `preFailoverScript` to slurm.takeover.sh
   - Enables takeover mode (BCM 10: `--extra takeover`; BCM 11: `slurmctldstartpolicy TAKEOVER`)
5. Restarts slurmctld services

**Manual procedure:** See `docs/migrate-slurmctl-to-bcm.md` for step-by-step manual instructions.

---

### `backup-slurm-files.py`

Comprehensive backup of Slurm-related files from BCM clusters. Includes systemd unit files, custom prolog/epilog scripts, configuration files, and Lua plugins. Tracks symlinks in manifest for accurate restoration.

```bash
./backup-slurm-files.py                      # Backup all Slurm files
./backup-slurm-files.py -o /path/to/backup   # Custom output directory
./backup-slurm-files.py --restore <dir>      # Restore missing files from backup
./backup-slurm-files.py -v                   # Verbose output
```

**What it backs up:**
- Systemd unit files (slurm*, munge*, mysql*, mariadb*) from all Slurm nodes
- Custom prolog/epilog scripts from WLM settings (non-default paths)
- Prolog/epilog symlinks in `/cm/local/apps/slurm/var/{prologs,epilogs}/` and their targets
- Config files in Slurm etc directory (slurm.conf, gres.conf, cgroup.conf, topology.conf, etc.)
- SPANK plugins (plugstack.conf.d/*) and Lua plugins (cli_filter.lua, job_submit.lua)

**Restore behavior:** Only restores files that are missing; existing files are left untouched. Symlinks are recreated pointing to correct targets.

---

## Directory Structure

```
├── healthcheck-slurm.py        # Cluster health validation
├── backup-slurm-db.py          # DB backup/restore
├── backup-slurm-files.py       # Slurm files backup/restore
├── migrate-slurmdb-to-bcm.py   # DB migration to BCM heads
├── migrate-slurmctl-to-bcm.py  # Slurm controller migration to BCM heads
├── healthcheck-config.conf     # Healthcheck configuration
├── docs/                       # Upgrade plans and manual procedures
│   ├── slurm-upgrade-23.11-to-25.05-plan.md
│   ├── migrate-slurmdb-to-bcm.md   # Manual DB migration procedure
│   └── migrate-slurmctl-to-bcm.md  # Manual controller migration procedure
└── .docs/                      # Reference documentation (BCM admin manual)
```

## Requirements

- Run on BCM head node as root
- Passwordless SSH to all cluster nodes
- `cmsh` available at `/cm/local/apps/cmd/bin/cmsh`
- Python 3.8+

## Typical Upgrade Workflow

1. **Pre-upgrade:** `./healthcheck-slurm.py --pre-upgrade`
2. **Backup DB:** `./backup-slurm-db.py -o /root/slurm-upgrade-backups`
3. **Backup files:** `./backup-slurm-files.py -o /root/slurm-upgrade-backups`
4. **Perform upgrade** (follow `plans/slurm-upgrade-23.11-to-25.05-plan.md`)
5. **Restore missing files:** `./backup-slurm-files.py --restore /root/slurm-upgrade-backups/slurm-files-*`
6. **Post-upgrade:** `./healthcheck-slurm.py --post-upgrade`

## Troubleshooting

### Cluster ID Mismatch

If restoring a database from a different cluster (e.g., in a lab environment) and slurmctld fails to start with:

```
fatal: CLUSTER ID MISMATCH.
slurmctld has been started with "ClusterID=1340" from the state files in StateSaveLocation, 
but the DBD thinks it should be "2600".
```

**Fix:** Remove the clustername file to accept the cluster ID from the restored database:

```bash
rm /cm/shared/apps/slurm/var/cm/statesave/slurm/clustername
systemctl restart slurmctld
```

This tells slurmctld to use the cluster ID from the database instead of the cached state files.

### slurmdbd Fails with "Unmet Condition"

After migrating to BCM head nodes, if slurmdbd shows `ConditionPathExists=/etc/slurm/slurmdbd.conf was not met`:

**Fix:** Create the systemd drop-in file (the migration script does this automatically):

```bash
mkdir -p /etc/systemd/system/slurmdbd.service.d
cat > /etc/systemd/system/slurmdbd.service.d/99-cmd.conf << 'EOF'
[Unit]
ConditionPathExists=
[Service]
Environment=SLURM_CONF=/cm/shared/apps/slurm/var/etc/slurm/slurm.conf
EOF
systemctl daemon-reload
systemctl start slurmdbd
```

### slurmdbd Fails with "This host not configured to run SlurmDBD"

The slurmaccounting `primary` field in the cmdaemon database still points to the old controllers:

**Fix:** Use the migration script's re-update option:

```bash
./migrate-slurmdb-to-bcm.py --reupdate-primary
```

Or manually update (must stop cmdaemon first):

```bash
systemctl stop cmd
mysql cmdaemon -e "UPDATE Roles SET extra_values='{\"ha\":true,\"primary\":\"$(hostname -s)\"}' WHERE CAST(name AS CHAR)='slurmaccounting';"
systemctl start cmd
```

## License

Internal NVIDIA tooling for DGX SuperPOD clusters.

