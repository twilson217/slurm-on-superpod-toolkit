# Migrate Slurm Accounting Database to BCM Head Nodes

This document provides step-by-step instructions for manually migrating the Slurm accounting database from dedicated Slurm controllers to BCM head nodes. After migration, slurmdbd will run on the BCM head nodes with database HA provided by BCM's MySQL replication.

> **Note:** The `migrate-slurmdb-to-bcm.py` script automates this entire process. Use this manual procedure only if you need to understand the steps or troubleshoot issues.

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
8. Update slurm.conf if needed
9. Sync database to passive head node
10. Start services and verify

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

### View current setting
```bash
mysql cmdaemon -e "SELECT uuid, CAST(extra_values AS CHAR) as extra_values, CAST(name AS CHAR) as name FROM Roles WHERE CAST(name AS CHAR)='slurmaccounting';"
```

### Update the primary to the active BCM head node
```bash
PRIMARY_HEAD="travisw-j2-a"  # Replace with your active head node
mysql cmdaemon -e "UPDATE Roles SET extra_values='{\"ha\":true,\"primary\":\"${PRIMARY_HEAD}\"}' WHERE CAST(name AS CHAR)='slurmaccounting';"
```

### Restart cmdaemon to apply changes
```bash
systemctl restart cmd
sleep 10
```

### Verify the change
```bash
cmsh -c "configurationoverlay; use slurm-accounting; roles; use slurmaccounting; show" | grep primary
```

---

## Step 8: Update slurm.conf (If Needed)

Check if BCM's autogenerated section has the correct AccountingStorageHost:

```bash
grep -A 30 "BEGIN AUTOGENERATED" /cm/shared/apps/slurm/var/etc/slurm/slurm.conf | grep AccountingStorage
```

If correct, remove any duplicate entries outside the autogenerated section:

```bash
# View current state
grep AccountingStorage /cm/shared/apps/slurm/var/etc/slurm/slurm.conf

# Remove duplicates outside autogenerated section (be careful!)
sed -i '1,/BEGIN AUTOGENERATED/{ /^AccountingStorageHost=/d; /^AccountingStorageBackupHost=/d; }' /cm/shared/apps/slurm/var/etc/slurm/slurm.conf
```

If BCM's autogenerated section does NOT have correct values, add them manually after `AccountingStorageUser=slurm`:

```bash
# Edit the file
vi /cm/shared/apps/slurm/var/etc/slurm/slurm.conf

# Add these lines after AccountingStorageUser=slurm:
# AccountingStorageHost=travisw-j2-a
# AccountingStorageBackupHost=travisw-j2-b
```

---

## Step 9: Sync Database to Passive Head Node

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

## Step 10: Start Services and Verify

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

If slurmctld warns about duplicate entries:
```bash
# Remove duplicates outside autogenerated section
sed -i '1,/BEGIN AUTOGENERATED/{ /^AccountingStorageHost=/d; /^AccountingStorageBackupHost=/d; }' /cm/shared/apps/slurm/var/etc/slurm/slurm.conf
```

---

## Rollback Procedure

If migration fails and you need to revert:

1. Stop slurmdbd on head nodes:
   ```bash
   cmsh -c "device; foreach -l slurmaccounting (services; stop slurmdbd)"
   ```

2. Restore BCM configuration to point to original controllers:
   ```bash
   cmsh -c "configurationoverlay; use slurm-accounting; set allheadnodes no; set nodes slurmctl-01,slurmctl-02; commit"
   ```

3. Update slurmaccounting primary back to original:
   ```bash
   mysql cmdaemon -e "UPDATE Roles SET extra_values='{\"ha\":true,\"primary\":\"slurmctl-01\"}' WHERE CAST(name AS CHAR)='slurmaccounting';"
   systemctl restart cmd
   ```

4. Start slurmdbd on original controllers and verify.

---

## Post-Migration Checklist

- [ ] slurmdbd running on both BCM head nodes
- [ ] `sacctmgr show cluster` returns correct cluster
- [ ] `sacctmgr show account` shows accounts
- [ ] slurmctld running on Slurm controller nodes
- [ ] `sinfo` shows nodes in expected states
- [ ] `cmha status` shows healthy MySQL replication
- [ ] Job submission works: `srun hostname`

