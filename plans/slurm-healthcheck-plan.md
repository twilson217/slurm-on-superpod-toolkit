# Slurm Healthcheck Script Plan

## Overview

Create a comprehensive automated healthcheck script that validates all critical aspects of the Slurm cluster. This script will be used before and after upgrades to quickly identify issues and validate successful deployment.

## Healthcheck Components

### 1. System Information Collection
- Slurm version (sinfo --version)
- BCM version
- Operating system details
- Timestamp and hostname
- Current user running the check

### 2. Service Status Checks
- **slurmctld** status on both head nodes
- **slurmdbd** status on head nodes
- **slurmd** status on all compute nodes
- Service uptime and restart counts
- Check if services are enabled at boot

### 3. Node Health Validation
- Total node count vs expected count
- Node states (idle, allocated, down, drain, etc.)
- Identify any nodes in error states
- Check for nodes not responding
- Verify all nodes report correct slurmd version
- Check node features/GRES configuration

### 4. Database and Accounting
- slurmdbd connectivity test
- Database connection status
- Accounting storage validation
- Recent job history query (sacct)
- Cluster/partition/account listing

### 5. Configuration Validation
- slurm.conf syntax check
- slurmdbd.conf validation
- Check for configuration consistency across nodes
- Verify critical parameters (ControlMachine, AccountingStorageHost, etc.)
- Validate partition configurations

### 6. Job Submission Tests
- **Basic job test**: Simple hostname command
- **Multi-node job test**: If cluster has multiple nodes
- **Interactive job test**: srun command
- **Batch job test**: sbatch submission
- **Job cancellation test**: Verify scancel works
- Measure job start latency

### 7. Pyxis/Enroot Container Tests
- Check if Pyxis is configured
- Verify enroot installation
- Test container image pull
- Run simple containerized job
- Validate container cleanup

### 8. GPU/GRES Validation (if applicable)
- Check GPU detection
- Verify GRES configuration
- Test GPU job submission
- Validate GPU allocation

### 9. Resource Limits and Quotas
- Check partition limits
- Verify QOS settings
- Test resource allocation logic

### 10. Log File Analysis
- Check for recent ERROR/FATAL messages in slurmctld.log
- Check slurmdbd.log for issues
- Check slurmd.log on sample nodes
- Report any suspicious patterns

### 11. Network Connectivity
- Ping test between head nodes and compute nodes
- Check Slurm communication ports (6817, 6818, 6819)
- Verify munge is working

### 12. Performance Metrics
- Job submission response time
- sinfo command response time
- Node count and responsiveness
- Queue depth

## Script Structure

### Output Format
```
=============================================================
SLURM CLUSTER HEALTHCHECK
=============================================================
Timestamp: YYYY-MM-DD HH:MM:SS
Hostname: <hostname>
Slurm Version: X.XX.X
Run by: <username>
=============================================================

[PASS/FAIL/WARN] Category: Test Name
  Details: <information>
  
SUMMARY:
  Total Tests: XX
  Passed: XX
  Failed: XX
  Warnings: XX
  
Overall Status: [HEALTHY/DEGRADED/CRITICAL]
=============================================================
```

### Exit Codes
- 0: All checks passed
- 1: One or more warnings (non-critical)
- 2: One or more critical failures

### Script Features
- Color-coded output (green=pass, yellow=warn, red=fail)
- Verbose mode (-v) for detailed output
- Quiet mode (-q) for summary only
- JSON output mode (--json) for parsing
- Save results to file (--output)
- Pre-upgrade state capture (--pre-upgrade)
- Post-upgrade comparison (--post-upgrade)

## Pre-Upgrade State Capture

The `--pre-upgrade` option captures critical state and configuration data to a baseline file:

### Captured Data
1. **Accounting Database State**:
   - All users (sacctmgr show user -P)
   - All accounts (sacctmgr show account -P)
   - All QOS settings (sacctmgr show qos -P)
   - All associations (sacctmgr show associations -P)
   - TRES configuration (sacctmgr show tres -P)
   - Clusters (sacctmgr show cluster -P)
   - Coordinators and limits

2. **Configuration State**:
   - Partitions and their settings
   - Node configurations and features
   - GRES/GPU configurations
   - Job submission defaults
   - Priority weights and factors

3. **System State**:
   - List of all nodes and their states
   - Running/pending job counts
   - Node count per state (idle, allocated, down, etc.)
   - Slurm version information

4. **Historical Data Counts**:
   - Total job count in database
   - Job count per user/account (last 30 days)
   - Oldest job record timestamp

### Post-Upgrade Comparison

The `--post-upgrade` option reads the baseline file and validates:
- **No data loss**: All users, accounts, QOS still exist
- **Configuration preservation**: Partitions, GRES configs unchanged
- **Job history intact**: No gaps in accounting records
- **Count validation**: User/account/job counts match or increased (never decreased)
- **Version upgrade confirmed**: Slurm version changed as expected

## Implementation Files

1. **slurm-healthcheck.sh** - Main healthcheck script
2. **healthcheck-config.conf** - Configuration file for customizing checks
3. **README-healthcheck.md** - Documentation for using the script

## Usage Examples

```bash
# Capture pre-upgrade baseline
./slurm-healthcheck.sh --pre-upgrade --output slurm-baseline-23.11.json

# Run full healthcheck during operation
./slurm-healthcheck.sh

# Validate post-upgrade and compare to baseline
./slurm-healthcheck.sh --post-upgrade --baseline slurm-baseline-23.11.json

# Verbose post-upgrade validation
./slurm-healthcheck.sh --post-upgrade --baseline slurm-baseline-23.11.json -v

# Just run comparison without other checks
./slurm-healthcheck.sh --compare-only --baseline slurm-baseline-23.11.json

# Regular healthcheck with output
./slurm-healthcheck.sh --output post-upgrade-check.txt

# JSON output for automation
./slurm-healthcheck.sh --json > healthcheck.json
```

## Integration with Upgrade Process

The healthcheck should be run:
1. **Before starting upgrade** - Establish baseline with `--pre-upgrade`
2. **After completing 24.11 upgrade** - Validate intermediate state with `--post-upgrade`
3. **After completing 25.05 upgrade** - Validate final state with `--post-upgrade`
4. **Post-upgrade validation period** - Daily checks for a week

## Key Validations by Upgrade Phase

### Pre-Upgrade (Baseline)
- Document current version
- Verify all services running
- Confirm all nodes responsive
- Test job submission works
- Validate Pyxis functionality
- Check accounting database
- **Capture all accounting data to baseline file**

### Post-Upgrade (Validation)
- Verify new version installed
- Confirm all services restarted successfully
- Check all nodes upgraded and responsive
- Validate job submission still works
- Test Pyxis with new Slurm version
- Confirm accounting continuity
- Compare performance metrics with baseline
- **Validate no accounting data lost (users, accounts, QOS, job history)**

## Script Requirements

- Must run on head node as root or slurm user
- Requires: sinfo, squeue, sacct, scontrol, sacctmgr, pdsh
- Optional: jq (for JSON processing)
- Must have cmsh access for BCM-specific checks
- Network access to all compute nodes

## Error Handling

- Graceful degradation if optional checks fail
- Clear error messages for failures
- Suggestions for remediation when possible
- Safe to run on production systems (read-only checks)
- Timeout protection for hung commands

## Extensibility

- Modular design for easy addition of new checks
- Plugin architecture for custom validations
- Configuration file to enable/disable specific tests
- Support for customer-specific requirements

## To-do Items

- [ ] Create main slurm-healthcheck.sh script with all test functions
- [ ] Implement --pre-upgrade baseline capture functionality
- [ ] Implement --post-upgrade comparison functionality  
- [ ] Create healthcheck-config.conf configuration file
- [ ] Create README-healthcheck.md documentation
- [ ] Add JSON output format support
- [ ] Add verbose and quiet modes

