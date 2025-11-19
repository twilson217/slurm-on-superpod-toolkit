# Slurm Healthcheck Script

A comprehensive Python-based healthcheck tool for validating Slurm cluster health before and after upgrades. This tool provides automated testing of all critical Slurm components with special focus on preventing data loss during upgrades.

## Features

- **Comprehensive Checks**: Tests services, nodes, accounting database, job submission, Pyxis/Enroot, logs, and authentication
- **Pre-Upgrade Baseline Capture**: Records complete cluster state including all accounting data
- **Post-Upgrade Validation**: Compares current state to baseline to detect any data loss
- **Multiple Output Formats**: Human-readable colored output, verbose mode, quiet mode, and JSON
- **Exit Codes**: Proper exit codes for automation and CI/CD integration
- **Safe Operation**: All checks are read-only except for optional job submission tests

## Requirements

- Python 3.12+ (tested with 3.12.3)
- Slurm commands: `sinfo`, `squeue`, `sacct`, `sacctmgr`, `scontrol`, `srun`, `sbatch`
- System tools: `systemctl`, `pdsh` (optional), `munge`
- Permissions: Must run as root or slurm user for full checks
- Network access to compute nodes

### Python Dependencies

The script uses only standard library modules:
- `argparse`, `json`, `subprocess`, `sys`, `os`, `time`, `re`
- `datetime`, `pathlib`, `typing`, `dataclasses`, `enum`

No external Python packages required!

## Installation

```bash
# Make the script executable
chmod +x slurm-healthcheck.py

# Optionally, copy to system path
sudo cp slurm-healthcheck.py /usr/local/bin/slurm-healthcheck

# Optionally, copy configuration file
sudo mkdir -p /etc/slurm-healthcheck
sudo cp healthcheck-config.conf /etc/slurm-healthcheck/
```

## Quick Start

### Basic Healthcheck

Run a standard healthcheck to validate current cluster status:

```bash
./slurm-healthcheck.py
```

### Pre-Upgrade Workflow

Before starting an upgrade, capture a baseline:

```bash
# Capture baseline with automatic filename
./slurm-healthcheck.py --pre-upgrade -o slurm-baseline-23.11.json

# This captures:
# - All users, accounts, QOS, associations, TRES
# - Partition and node configurations
# - Job history for the last 30 days
# - Current system state
```

### Post-Upgrade Workflow

After completing an upgrade, validate against the baseline:

```bash
# Compare current state to baseline
./slurm-healthcheck.py --post-upgrade --baseline slurm-baseline-23.11.json

# With verbose output
./slurm-healthcheck.py --post-upgrade --baseline slurm-baseline-23.11.json -v

# Save comparison results
./slurm-healthcheck.py --post-upgrade --baseline slurm-baseline-23.11.json -o post-upgrade-results.txt
```

## Usage Examples

### Standard Operations

```bash
# Regular healthcheck
./slurm-healthcheck.py

# Verbose output (shows all details)
./slurm-healthcheck.py -v

# Quiet mode (summary only)
./slurm-healthcheck.py -q

# Save results to file
./slurm-healthcheck.py -o healthcheck-report.txt

# JSON output for parsing/automation
./slurm-healthcheck.py --json -o results.json

# Disable colors (for log files)
./slurm-healthcheck.py --no-color
```

### Upgrade Workflow

```bash
# Step 1: Before upgrade (capture baseline)
./slurm-healthcheck.py --pre-upgrade -o baseline-before-23.11-to-24.11.json

# Step 2: After first upgrade (23.11 → 24.11)
./slurm-healthcheck.py --post-upgrade --baseline baseline-before-23.11-to-24.11.json

# Step 3: Before second upgrade (capture new baseline)
./slurm-healthcheck.py --pre-upgrade -o baseline-before-24.11-to-25.05.json

# Step 4: After second upgrade (24.11 → 25.05)
./slurm-healthcheck.py --post-upgrade --baseline baseline-before-24.11-to-25.05.json
```

### Comparison Only

Run only the baseline comparison without other checks:

```bash
./slurm-healthcheck.py --compare-only --baseline slurm-baseline-23.11.json
```

## Command-Line Options

```
usage: slurm-healthcheck.py [-h] [-v] [-q] [--json] [--no-color] [-o OUTPUT]
                            [--pre-upgrade] [--post-upgrade] [-b BASELINE]
                            [--compare-only]

optional arguments:
  -h, --help            Show help message and exit
  -v, --verbose         Verbose output with detailed information
  -q, --quiet           Quiet mode, only show summary
  --json                Output results in JSON format
  --no-color            Disable colored output
  -o OUTPUT, --output OUTPUT
                        Save results to file
  --pre-upgrade         Capture pre-upgrade baseline state
  --post-upgrade        Run post-upgrade validation with baseline comparison
  -b BASELINE, --baseline BASELINE
                        Baseline file for comparison (used with --post-upgrade)
  --compare-only        Only run baseline comparison, skip other checks
```

## Check Categories

The healthcheck performs the following categories of tests:

### 1. System Checks
- Slurm version detection
- Operating system information
- User and hostname identification

### 2. Service Status
- `slurmctld` (controller) status and uptime
- `slurmdbd` (database) status and uptime
- Service enable status

### 3. Node Health
- Total node count
- Node state distribution (idle, allocated, down, drain, etc.)
- Problem node identification
- Node version consistency

### 4. Accounting Database
- `slurmdbd` connectivity
- Database connection validation
- Job history access
- Accounting storage verification

### 5. Configuration Validation
- Partition configuration
- Node configuration
- GRES/GPU settings
- Configuration file syntax

### 6. Job Submission Tests
- Basic job submission (`srun`)
- Job execution validation
- Response time measurement

### 7. Pyxis/Enroot Tests
- Pyxis plugin detection
- Enroot installation check
- Container capability validation

### 8. Authentication
- Munge service status
- Munge encode/decode test

### 9. Log Analysis
- Recent error detection in `slurmctld.log`
- Recent error detection in `slurmdbd.log`
- Warning pattern identification

### 10. Baseline Comparison (with --post-upgrade)
- Slurm version verification
- User count validation (no loss)
- Account count validation (no loss)
- QOS preservation check
- TRES preservation check
- Job history integrity
- Partition preservation
- Node count validation

## Output Format

### Standard Output

```
=================================================================
SLURM CLUSTER HEALTHCHECK
=================================================================
Timestamp: 2025-11-19 14:30:00
Hostname: headnode-01
User: root
Slurm Version: slurm 24.11.0
=================================================================

[PASS] System: Slurm Version Check
[PASS] Services: Slurm Controller Status
[PASS] Services: Slurm Database Daemon Status
[PASS] Nodes: Node Health Check
[PASS] Accounting: Database Connection
[WARN] Logs: Controller Log Check
  → Found 2 error/warning line(s) in last 100 lines

=================================================================
SUMMARY
=================================================================
Total Tests: 15
Passed: 13
Failed: 0
Warnings: 2
Skipped: 0

Overall Status: DEGRADED
=================================================================
```

### JSON Output

```json
{
  "timestamp": "2025-11-19T14:30:00.123456",
  "hostname": "headnode-01",
  "slurm_version": "slurm 24.11.0",
  "user": "root",
  "summary": {
    "total": 15,
    "passed": 13,
    "failed": 0,
    "warnings": 2,
    "skipped": 0
  },
  "overall_status": "DEGRADED",
  "tests": [
    {
      "category": "System",
      "name": "Slurm Version Check",
      "status": "PASS",
      "message": "Slurm is installed: slurm 24.11.0",
      "details": {
        "version": "slurm 24.11.0"
      }
    }
  ]
}
```

## Exit Codes

The script uses standard exit codes for automation:

- **0**: All checks passed (HEALTHY)
- **1**: One or more warnings (DEGRADED)
- **2**: One or more critical failures (CRITICAL) or usage errors

Example automation usage:

```bash
#!/bin/bash
./slurm-healthcheck.py
exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "Cluster is healthy, proceeding with upgrade"
elif [ $exit_code -eq 1 ]; then
    echo "Cluster has warnings, review before proceeding"
    exit 1
else
    echo "Cluster has critical issues, aborting"
    exit 2
fi
```

## Integration with Slurm Upgrade

### Recommended Workflow

```bash
# =============================================================================
# BEFORE STARTING UPGRADE
# =============================================================================

# 1. Run healthcheck to ensure cluster is healthy
./slurm-healthcheck.py -v

# 2. Capture baseline
./slurm-healthcheck.py --pre-upgrade -o baseline-23.11-$(date +%Y%m%d).json

# 3. Backup the baseline file
cp baseline-23.11-*.json /backup/location/

# =============================================================================
# AFTER FIRST UPGRADE (23.11 → 24.11)
# =============================================================================

# 4. Run post-upgrade validation
./slurm-healthcheck.py --post-upgrade --baseline baseline-23.11-*.json -v

# 5. Run full healthcheck
./slurm-healthcheck.py -v -o post-24.11-healthcheck.txt

# 6. Capture new baseline before next upgrade
./slurm-healthcheck.py --pre-upgrade -o baseline-24.11-$(date +%Y%m%d).json

# =============================================================================
# AFTER SECOND UPGRADE (24.11 → 25.05)
# =============================================================================

# 7. Run post-upgrade validation
./slurm-healthcheck.py --post-upgrade --baseline baseline-24.11-*.json -v

# 8. Final comprehensive healthcheck
./slurm-healthcheck.py -v -o final-25.05-healthcheck.txt

# 9. Compare to original baseline (optional)
./slurm-healthcheck.py --compare-only --baseline baseline-23.11-*.json
```

## Troubleshooting

### Command Not Found Errors

If you see "Command not found" errors:

```bash
# Add Slurm to PATH
export PATH="/cm/shared/apps/slurm/current/bin:$PATH"

# Or load Slurm module
module load slurm

# Then run healthcheck
./slurm-healthcheck.py
```

### Permission Denied

The script requires appropriate permissions:

```bash
# Run as root
sudo ./slurm-healthcheck.py

# Or run as slurm user
sudo -u slurm ./slurm-healthcheck.py
```

### Timeout Issues

If commands are timing out:

```bash
# Check Slurm services are running
systemctl status slurmctld
systemctl status slurmdbd

# Check network connectivity
ping <compute-node>

# Check Slurm communication
sinfo
squeue
```

### Baseline File Issues

If you get baseline file errors:

```bash
# Verify file exists
ls -lh baseline-*.json

# Check JSON is valid
python3 -m json.tool baseline-*.json > /dev/null

# View baseline content
cat baseline-*.json | python3 -m json.tool | less
```

## Baseline File Format

The baseline JSON file contains:

```json
{
  "timestamp": "2025-11-19T14:00:00",
  "hostname": "headnode-01",
  "user": "root",
  "slurm_version": "slurm 23.11.0",
  "accounting": {
    "users": ["user1|account1|...", "user2|account2|..."],
    "accounts": ["account1|description|...", "account2|..."],
    "qos": ["normal|...", "high|..."],
    "associations": [...],
    "tres": [...],
    "clusters": ["cluster1|..."],
    "job_count_30days": 1234,
    "user_job_counts": {"user1": 100, "user2": 50},
    "account_job_counts": {"account1": 150}
  },
  "configuration": {
    "partitions": {
      "compute": {"available": "up", "timelimit": "24:00:00", "nodes": "32"}
    },
    "nodes": {
      "node001": {"state": "idle", "gres": "gpu:8"}
    }
  },
  "system_state": {
    "total_nodes": 32,
    "node_state_counts": {"idle": 30, "allocated": 2}
  }
}
```

## Configuration File

The `healthcheck-config.conf` file allows customization of:
- Check timeouts
- Enable/disable specific check categories
- Log file locations
- Warning/failure thresholds
- Pyxis plugin paths
- Job test parameters

See `healthcheck-config.conf` for full documentation.

## Best Practices

1. **Always capture baseline before upgrades**
2. **Keep baseline files in version control or backup location**
3. **Run healthchecks during maintenance windows to validate cluster**
4. **Use verbose mode (-v) for troubleshooting**
5. **Save results to files for audit trails**
6. **Integrate into automation/CI-CD pipelines**
7. **Run daily healthchecks to catch issues early**
8. **Compare post-upgrade results to pre-upgrade baseline**

## Automation Example

Cron job for daily healthchecks:

```bash
# /etc/cron.daily/slurm-healthcheck
#!/bin/bash

LOGDIR="/var/log/slurm-healthcheck"
mkdir -p "$LOGDIR"

DATE=$(date +%Y%m%d-%H%M%S)
LOGFILE="$LOGDIR/healthcheck-$DATE.json"

/usr/local/bin/slurm-healthcheck --json -o "$LOGFILE"

# Keep last 30 days of logs
find "$LOGDIR" -name "healthcheck-*.json" -mtime +30 -delete

# Send alert if critical
if [ $? -eq 2 ]; then
    echo "CRITICAL: Slurm healthcheck failed" | mail -s "Slurm Alert" admin@example.com
fi
```

## Support and Contributing

For issues, questions, or contributions, please contact your system administrator or refer to the Slurm documentation.

## License

This script is provided as-is for use with NVIDIA BCM-managed Slurm clusters.

## Version

Version: 1.0.0
Last Updated: 2025-11-19

