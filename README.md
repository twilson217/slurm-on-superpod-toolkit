# Slurm on SuperPOD Toolkit

Tools for managing Slurm upgrades, healthchecks, backups, and migrations in BCM-managed DGX SuperPOD clusters.

## Scripts

### `slurm-healthcheck.py`

Comprehensive health validation for Slurm clusters. Run before/after upgrades to verify cluster state.

```bash
./slurm-healthcheck.py                    # Full healthcheck
./slurm-healthcheck.py --maint            # Skip job submission tests (maintenance mode)
./slurm-healthcheck.py --pre-upgrade      # Capture baseline before upgrade
./slurm-healthcheck.py --post-upgrade     # Compare against baseline after upgrade
```

**Features:** Service status, node health, HA validation, partition checks, Pyxis/Enroot, GPU/GRES, log analysis, database backup validation.

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
./migrate-slurmdb-to-bcm.py
```

**What it does:**
1. Dumps database from current `StorageHost`
2. Imports into local MariaDB on BCM head node
3. Updates `slurmaccounting` role via cmsh (primary, storagehost)
4. Updates overlay to use `allheadnodes yes`
5. Restarts slurmdbd services

---

### `backup-slurm-unitfiles.py`

Backup systemd unit files for Slurm services from all relevant nodes. Useful before upgrades since unit files may be modified.

```bash
./backup-slurm-unitfiles.py
```

Creates timestamped directory `slurm-unitfiles-<version>/` with unit files from all nodes with Slurm roles.

---

## Directory Structure

```
├── slurm-healthcheck.py        # Cluster health validation
├── backup-slurm-db.py          # DB backup/restore
├── migrate-slurmdb-to-bcm.py   # DB migration to BCM heads
├── backup-slurm-unitfiles.py   # Systemd unit file backup
├── healthcheck-config.conf     # Healthcheck configuration
├── plans/                      # Upgrade and healthcheck plans
│   ├── slurm-upgrade-23.11-to-25.05-plan.md
│   └── slurm-healthcheck-plan.md
└── .docs/                      # Reference documentation
```

## Requirements

- Run on BCM head node as root
- Passwordless SSH to all cluster nodes
- `cmsh` available at `/cm/local/apps/cmd/bin/cmsh`
- Python 3.8+

## Typical Upgrade Workflow

1. **Pre-upgrade:** `./slurm-healthcheck.py --pre-upgrade`
2. **Backup DB:** `./backup-slurm-db.py -o /root/slurm-upgrade-backups`
3. **Backup unit files:** `./backup-slurm-unitfiles.py`
4. **Perform upgrade** (follow `plans/slurm-upgrade-23.11-to-25.05-plan.md`)
5. **Post-upgrade:** `./slurm-healthcheck.py --post-upgrade`

## License

Internal NVIDIA tooling for DGX SuperPOD clusters.

