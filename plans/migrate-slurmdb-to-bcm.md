# Migrate Slurm Accounting Database to BCM Head Nodes

This document provides step-by-step instructions for manually migrating the Slurm accounting database from dedicated Slurm controllers to BCM head nodes. After migration, slurmdbd will run on the BCM head nodes with database HA provided by BCM's MySQL replication.

> **Note:** The `migrate-slurmdb-to-bcm.py` script automates this entire process. Use this manual procedure only if you need to understand the steps or troubleshoot issues.
>
> **Script Options:**
> - `--reupdate-primary` - Re-run only the cmdaemon database update for slurmaccounting primary
> - `--rollback --original-primary <host> [--original-backup <host>]` - Rollback migration to original controllers

## Prerequisites

- Run all commands on the **active BCM head node** as root
- Passwordless SSH access to all Slurm nodes
- `cmsh` available at `/cm/local/apps/cmd/bin/cmsh`
- MySQL/MariaDB client installed on BCM head nodes

## Overview

1. Gather current configuration
2. Stop slurmdbd services
3. Dump the source database
4. Import database to BCM head node
5. Configure MySQL user permissions
6. Update BCM configuration via cmsh
7. Update slurmaccounting primary in cmdaemon database
8. Configure slurmdbd systemd drop-in file on head nodes
9. Update slurm.conf if needed
10. Sync database to passive head node
11. Start services and verify

---

## Step 1: Gather Current Configuration

### Identify current database location
```bash
cat /cm/shared/apps/slurm/var/etc/slurmdbd.conf | grep -E "^(StorageHost|StoragePort|StorageLoc|StorageUser|StoragePass|DbdHost)"
```

Save the output - you'll need these values:
- `StorageHost` - Current database server
- `StorageLoc` - Database name (usually `slurm_acct_db`)
- `StorageUser` - MySQL username (usually `slurm`)
- `StoragePass` - MySQL password

### Identify BCM head nodes
```bash
cmha status
```

Note which node has `*` (active) and which is passive.

---

## Step 2: Stop slurmdbd Services

### Discover nodes running slurmdbd
```bash
cmsh -c "device; foreach -l slurmaccounting (get hostname)"
```

### Stop slurmdbd on all nodes via cmsh
```bash
cmsh -c "device; foreach -l slurmaccounting (services; stop slurmdbd)"
```

### Verify services are stopped
```bash
cmsh -c "device; foreach -l slurmaccounting (services; status slurmdbd)"
```

---

## Step 3: Dump the Source Database

### Set variables (replace with your values)
```bash
SOURCE_HOST="slurmctl-01"  # Current StorageHost
DB_NAME="slurm_acct_db"
DB_USER="slurm"
DB_PASS="your_password_here"
DUMP_FILE="/root/slurm-db-migration/slurm_acct_db-$(date +%Y%m%d-%H%M%S).sql"

mkdir -p /root/slurm-db-migration
```

### Dump the database
```bash
mysqldump -h ${SOURCE_HOST} -u ${DB_USER} -p"${DB_PASS}" \
  --single-transaction \
  --routines \
  --triggers \
  --events \
  --default-character-set=utf8mb4 \
  ${DB_NAME} > ${DUMP_FILE}
```

### Verify dump was created
```bash
ls -lh ${DUMP_FILE}
head -20 ${DUMP_FILE}
```

---

## Step 4: Import Database to BCM Head Node

### Create the database if it doesn't exist
```bash
mysql -e "CREATE DATABASE IF NOT EXISTS ${DB_NAME} DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
```

### Import the dump
```bash
mysql --default-character-set=utf8mb4 ${DB_NAME} < ${DUMP_FILE}
```

### Verify import
```bash
mysql ${DB_NAME} -e "SHOW TABLES;" | wc -l
```

---

## Step 5: Configure MySQL User Permissions

### On the active BCM head node
```bash
mysql -e "ALTER USER '${DB_USER}'@'%' IDENTIFIED BY '${DB_PASS}'; FLUSH PRIVILEGES;"
```

### On the passive BCM head node (required for cmha dbreclone)
```bash
PASSIVE_HEAD="travisw-j2-b"  # Replace with your passive head node
ssh ${PASSIVE_HEAD} "mysql -e \"ALTER USER '${DB_USER}'@'%' IDENTIFIED BY '${DB_PASS}'; FLUSH PRIVILEGES;\""
```

### Verify MySQL access works
```bash
mysql -u ${DB_USER} -p"${DB_PASS}" -e "SELECT 1;"
```

---

## Step 6: Update BCM Configuration via cmsh

### Find the slurmaccounting overlay
```bash
cmsh -c "configurationoverlay; list" | grep slurmaccounting
```

Note the overlay name (usually `slurm-accounting`).

### Update slurmaccounting role settings
```bash
OVERLAY_NAME="slurm-accounting"
cmsh -c "configurationoverlay; use ${OVERLAY_NAME}; roles; use slurmaccounting; set storagehost master; commit"
```

### Update overlay to use all head nodes
```bash
cmsh -c "configurationoverlay; use ${OVERLAY_NAME}; set nodes; set allheadnodes yes; commit"
```

---

## Step 7: Update slurmaccounting Primary in cmdaemon Database

The `primary` field for the slurmaccounting role is stored as JSON in the cmdaemon database and cannot be set via cmsh.

> **Important:** You must **STOP** cmdaemon before updating the database, then **START** it afterward. If you update while cmdaemon is running and then restart, cmdaemon may overwrite your database change with its cached in-memory state.

### View current setting
```bash
mysql cmdaemon -e "SELECT uuid, CAST(extra_values AS CHAR) as extra_values, CAST(name AS CHAR) as name FROM Roles WHERE CAST(name AS CHAR)='slurmaccounting';"
```

### Stop cmdaemon before updating
```bash
systemctl stop cmd
sleep 2
```

### Update the primary to the active BCM head node
```bash
PRIMARY_HEAD="travisw-j2-a"  # Replace with your active head node
mysql cmdaemon -e "UPDATE Roles SET extra_values='{\"ha\":true,\"primary\":\"${PRIMARY_HEAD}\"}' WHERE CAST(name AS CHAR)='slurmaccounting';"
```

### Verify the database update was applied
```bash
mysql cmdaemon -N -e "SELECT CAST(extra_values AS CHAR) FROM Roles WHERE CAST(name AS CHAR)='slurmaccounting';"
```

Expected output should show your head node:
```
{"ha":true,"primary":"travisw-j2-a"}
```

### Start cmdaemon to load the updated configuration
```bash
systemctl start cmd
sleep 10
```

### Verify the change via cmsh
```bash
cmsh -c "configurationoverlay; use slurm-accounting; roles; use slurmaccounting; show" | grep primary
```

### If the update didn't persist

If the `primary` value reverts to the old value after starting cmdaemon, repeat the stop/update/start sequence. The script's `--reupdate-primary` option automates this:

```bash
./migrate-slurmdb-to-bcm.py --reupdate-primary
```

---

## Step 8: Configure slurmdbd systemd Drop-in File on Head Nodes

The default slurmdbd systemd unit file has `ConditionPathExists=/etc/slurm/slurmdbd.conf`, but BCM uses `/cm/shared/apps/slurm/var/etc/slurmdbd.conf`. A drop-in file is needed to clear this condition so the service can start.

> **Note:** This drop-in file may already exist if slurmdbd was previously configured on these head nodes. Check first before creating.

### Check if drop-in file exists on active head node
```bash
ls -la /etc/systemd/system/slurmdbd.service.d/99-cmd.conf 2>/dev/null && echo "File exists" || echo "File missing"
```

### Create drop-in file on active head node (if missing)
```bash
mkdir -p /etc/systemd/system/slurmdbd.service.d
cat > /etc/systemd/system/slurmdbd.service.d/99-cmd.conf << 'EOF'
[Unit]
ConditionPathExists=
[Service]
Environment=SLURM_CONF=/cm/shared/apps/slurm/var/etc/slurm/slurm.conf
EOF
systemctl daemon-reload
```

### Create drop-in file on passive head node (if missing)
```bash
PASSIVE_HEAD="travisw-j2-b"  # Replace with your passive head node

ssh ${PASSIVE_HEAD} 'mkdir -p /etc/systemd/system/slurmdbd.service.d && cat > /etc/systemd/system/slurmdbd.service.d/99-cmd.conf << EOF
[Unit]
ConditionPathExists=
[Service]
Environment=SLURM_CONF=/cm/shared/apps/slurm/var/etc/slurm/slurm.conf
EOF
systemctl daemon-reload'
```

### Verify drop-in is loaded
```bash
systemctl cat slurmdbd | grep -A2 "99-cmd.conf"
```

Expected output:
```
# /etc/systemd/system/slurmdbd.service.d/99-cmd.conf
[Unit]
ConditionPathExists=
```

---

## Step 9: Verify slurm.conf and Clean Up Duplicates

After updating the slurmaccounting primary in the cmdaemon database (Step 7), BCM's autogenerated section should now have the correct `AccountingStorageHost` and `AccountingStorageBackupHost` values.

### Verify the autogenerated section has correct values

```bash
grep -A 30 "BEGIN AUTOGENERATED" /cm/shared/apps/slurm/var/etc/slurm/slurm.conf | grep AccountingStorage
```

Expected output:
```
AccountingStorageHost=travisw-j2-a
AccountingStorageBackupHost=travisw-j2-b
```

### Remove any duplicate entries outside autogenerated section

If there are entries outside the autogenerated section (from previous manual edits), remove them to avoid duplicate warnings:

```bash
# View current state - look for duplicates
grep AccountingStorage /cm/shared/apps/slurm/var/etc/slurm/slurm.conf

# Remove duplicates outside autogenerated section
sed -i '1,/BEGIN AUTOGENERATED/{ /^AccountingStorageHost=/d; /^AccountingStorageBackupHost=/d; }' /cm/shared/apps/slurm/var/etc/slurm/slurm.conf
```

### If autogenerated section still has wrong values

If the autogenerated section doesn't have the correct values after Step 7:

1. Verify the `primary` was set correctly in the Roles table:
   ```bash
   mysql cmdaemon -e "SELECT CAST(extra_values AS CHAR) FROM Roles WHERE CAST(name AS CHAR)='slurmaccounting';"
   ```

2. Restart cmdaemon again:
   ```bash
   systemctl restart cmd
   sleep 10
   ```

3. Verify in cmsh:
   ```bash
   cmsh -c "configurationoverlay; use slurm-accounting; roles; use slurmaccounting; show" | grep primary
   ```

> **Note:** Do NOT manually add `AccountingStorageHost` entries outside the autogenerated section. This will cause duplicate warnings when BCM eventually regenerates the file. Always fix the source (the cmdaemon database) so BCM generates the correct values.

---

## Step 10: Sync Database to Passive Head Node

### Verify MySQL HA status
```bash
cmha status
```

### Clone database to passive head node
```bash
PASSIVE_HEAD="travisw-j2-b"  # Replace with your passive head node
cmha dbreclone ${PASSIVE_HEAD}
```

> **Note:** This can take 20-30+ minutes for large databases. The Slurm accounting database may be several hundred MB.

---

## Step 11: Start Services and Verify

### Start slurmdbd services
```bash
cmsh -c "device; foreach -l slurmaccounting (services; start slurmdbd)"
```

### Verify slurmdbd is running and listening
```bash
systemctl status slurmdbd
ss -tlnp | grep 6819
```

### Test Slurm accounting
```bash
sacctmgr show cluster
sacctmgr show account | head -10
sacctmgr show user | head -10
```

### Restart slurmctld services
```bash
cmsh -c "device; foreach -l slurmserver (services; restart slurmctld)"
```

---

## Troubleshooting

### slurmdbd Fails to Start with "Unmet Condition"

If `systemctl status slurmdbd` shows:
```
Condition: start condition failed at ...
           └─ ConditionPathExists=/etc/slurm/slurmdbd.conf was not met
```

The systemd drop-in file is missing. See Step 8 to create it, or run:
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

If slurmdbd logs show:
```
fatal: This host not configured to run SlurmDBD ((hostname) != slurmctl-01 | (backup) slurmctl-02)
```

The `primary` field in the cmdaemon database still points to the old Slurm controllers. See Step 7 to update it, or use the script:
```bash
./migrate-slurmdb-to-bcm.py --reupdate-primary
```

### Cluster ID Mismatch

If slurmctld fails to start with:
```
fatal: CLUSTER ID MISMATCH.
slurmctld has been started with "ClusterID=1340" from the state files, but the DBD thinks it should be "2600".
```

**Fix:**
```bash
rm /cm/shared/apps/slurm/var/cm/statesave/slurm/clustername
systemctl restart slurmctld
```

### slurmdbd Not Listening on Port 6819

Check if slurmdbd can connect to MySQL:
```bash
tail -50 /var/log/slurmdbd
```

If you see `Access denied for user 'slurm'`:
```bash
mysql -e "ALTER USER 'slurm'@'%' IDENTIFIED BY 'your_password'; FLUSH PRIVILEGES;"
systemctl restart slurmdbd
```

### cmha dbreclone Fails at "Cloning workload manager databases"

The slurm MySQL user password on the passive head node doesn't match slurmdbd.conf:
```bash
PASSIVE_HEAD="travisw-j2-b"
DB_PASS="your_password_here"
ssh ${PASSIVE_HEAD} "mysql -e \"ALTER USER 'slurm'@'%' IDENTIFIED BY '${DB_PASS}'; FLUSH PRIVILEGES;\""
```

Then retry `cmha dbreclone`.

### Duplicate AccountingStorageHost Entries

If slurmctld warns about duplicate entries like:
```
error: AccountingStorageHost 1 specified more than once, latest value used
```

This means there are entries both inside and outside the autogenerated section. Remove the duplicates outside:
```bash
# View current state to see where duplicates are
grep -n AccountingStorage /cm/shared/apps/slurm/var/etc/slurm/slurm.conf

# Remove duplicates outside autogenerated section
sed -i '1,/BEGIN AUTOGENERATED/{ /^AccountingStorageHost=/d; /^AccountingStorageBackupHost=/d; }' /cm/shared/apps/slurm/var/etc/slurm/slurm.conf
```

> **Prevention:** Always fix the BCM configuration (cmdaemon database) rather than manually adding entries outside the autogenerated section. See Step 7 for the correct way to set the primary.

---

## Rollback Procedure

If migration fails and you need to revert:

### Using the script (recommended)
```bash
./migrate-slurmdb-to-bcm.py --rollback --original-primary slurmctl-01 --original-backup slurmctl-02
```

### Manual rollback

1. Stop slurmdbd on head nodes:
   ```bash
   cmsh -c "device; foreach -l slurmaccounting (services; stop slurmdbd)"
   ```

2. Restore BCM configuration to point to original controllers:
   ```bash
   cmsh -c "configurationoverlay; use slurm-accounting; set allheadnodes no; set nodes slurmctl-01,slurmctl-02; commit"
   ```

3. Update slurmaccounting primary back to original (stop cmdaemon first!):
   ```bash
   systemctl stop cmd
   sleep 2
   mysql cmdaemon -e "UPDATE Roles SET extra_values='{\"ha\":true,\"primary\":\"slurmctl-01\"}' WHERE CAST(name AS CHAR)='slurmaccounting';"
   systemctl start cmd
   sleep 10
   ```

4. Update storagehost back to original controller:
   ```bash
   cmsh -c "configurationoverlay; use slurm-accounting; roles; use slurmaccounting; set storagehost slurmctl-01; commit"
   ```

5. Start slurmdbd on original controllers and verify:
   ```bash
   ssh slurmctl-01 'systemctl status slurmdbd'
   sacctmgr show cluster
   ```

---

## Post-Migration Checklist

- [ ] systemd drop-in file exists on both head nodes: `/etc/systemd/system/slurmdbd.service.d/99-cmd.conf`
- [ ] slurmaccounting primary shows correct head node: `cmsh -c "configurationoverlay; use slurm-accounting; roles; use slurmaccounting; get primary"`
- [ ] slurmdbd running on both BCM head nodes: `systemctl status slurmdbd`
- [ ] slurmdbd listening on port 6819: `ss -tlnp | grep 6819`
- [ ] `sacctmgr show cluster` returns correct cluster
- [ ] `sacctmgr show account` shows accounts
- [ ] slurmctld running on Slurm controller nodes
- [ ] `sinfo` shows nodes in expected states
- [ ] `cmha status` shows healthy MySQL HA
- [ ] Job submission works: `srun hostname`
- [ ] Run healthcheck: `./healthcheck-slurm.py`

