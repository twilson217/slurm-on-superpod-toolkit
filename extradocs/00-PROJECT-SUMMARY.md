# Slurm Upgrade Project - Files Summary

## Project Overview

This directory contains all documentation and tools for upgrading Slurm from version 23.11 to 25.05 on a BCM-managed cluster with 31 compute nodes running Ubuntu 24.04.

## Files Created

### 1. Planning Documents

#### `slurm-upgrade-23.11-to-25.05-plan.md`
- **Purpose**: Complete step-by-step upgrade procedure
- **Content**: 
  - Two-step upgrade path (23.11 → 24.11 → 25.05)
  - Detailed commands for each step
  - Time estimates and outage windows
  - Rollback procedures
  - Key considerations and warnings

#### `slurm-healthcheck-plan.md`
- **Purpose**: Design specification for the healthcheck tool
- **Content**:
  - Healthcheck component breakdown
  - Pre/post-upgrade comparison strategy
  - Accounting data protection focus
  - Integration with upgrade workflow

### 2. Healthcheck Tool

#### `slurm-healthcheck.py` (Executable)
- **Purpose**: Automated cluster validation and upgrade verification tool
- **Language**: Python 3.12+ (no external dependencies)
- **Size**: ~41 KB
- **Features**:
  - Comprehensive cluster health checks
  - Pre-upgrade baseline capture
  - Post-upgrade comparison and validation
  - Accounting database integrity verification
  - Multiple output formats (text, JSON)
  - Color-coded results with exit codes
  - Safe, read-only operations

**Key Capabilities**:
- System checks (version, OS, services)
- Node health validation
- Accounting database verification (users, accounts, QOS, TRES, associations)
- Job submission testing
- Pyxis/Enroot container support testing
- Log analysis for errors
- Munge authentication testing
- Configuration validation

### 3. Configuration

#### `healthcheck-config.conf`
- **Purpose**: Customization options for healthcheck behavior
- **Content**:
  - Check enable/disable toggles
  - Timeout configurations
  - Threshold settings
  - Log file paths
  - Advanced options

### 4. Documentation

#### `README-healthcheck.md`
- **Purpose**: Complete user guide for the healthcheck tool
- **Content** (13 KB):
  - Installation instructions
  - Quick start guide
  - Detailed usage examples
  - Integration with upgrade workflow
  - Troubleshooting guide
  - Output format documentation
  - Best practices
  - Automation examples

## Quick Start Guide

### For the Slurm Upgrade:

```bash
# Read the upgrade plan
cat slurm-upgrade-23.11-to-25.05-plan.md | less

# Key information:
# - Total downtime: 4-6 hours (split into two 2-3 hour windows)
# - Must drain nodes and stop jobs during upgrade
# - Pyxis requires reinstallation after each upgrade
# - Image deployment: ~30-60 min per 10 nodes
```

### For Healthcheck Testing:

```bash
# 1. Test the healthcheck (before upgrade)
./slurm-healthcheck.py -v

# 2. Capture baseline before upgrade
./slurm-healthcheck.py --pre-upgrade -o baseline-23.11.json

# 3. After upgrade, validate no data loss
./slurm-healthcheck.py --post-upgrade --baseline baseline-23.11.json -v

# 4. View detailed documentation
cat README-healthcheck.md | less
```

## Upgrade Workflow Summary

### Phase 1: Pre-Upgrade
1. Run healthcheck to verify cluster is healthy
2. Capture baseline with `--pre-upgrade` flag
3. Backup baseline file to safe location
4. Review upgrade plan document

### Phase 2: First Upgrade (23.11 → 24.11)
1. Drain nodes and wait for jobs to complete
2. Stop Slurm services
3. Remove old packages, install new packages
4. Update BCM configuration
5. Deploy updated image to compute nodes
6. Reinstall Pyxis
7. Restart services and validate
8. Run healthcheck with baseline comparison

### Phase 3: Second Upgrade (24.11 → 25.05)
1. Capture new baseline
2. Repeat upgrade steps for 25.05
3. Validate with baseline comparison
4. Comprehensive final testing

## Important Notes

### Data Protection
The healthcheck tool specifically validates:
- **No loss of users** from accounting database
- **No loss of accounts** from accounting database
- **No loss of QOS settings**
- **No loss of TRES configurations**
- **No loss of job history** (validates counts)
- **No loss of associations**
- **Preservation of partitions** and node configurations

### Time Estimates
- **Image deployment formula**: 30-60 minutes per 10 nodes
- **31 nodes** = approximately 2-3 hours for full deployment
- **Total upgrade time**: 4-6 hours for both upgrades
- **Can be split** into two separate maintenance windows

### Critical Requirements
- Jobs must not be running during upgrade
- Both head nodes must be upgraded together
- All Slurm package versions must match
- Pyxis must be reinstalled after each major version change
- Test in lab environment first

## File Permissions

```bash
-rwxr-xr-x  slurm-healthcheck.py     # Executable
-rw-r--r--  healthcheck-config.conf  # Config file
-rw-r--r--  README-healthcheck.md    # Documentation
-rw-r--r--  slurm-*.md               # Plan documents
```

## Dependencies

### Slurm Upgrade Requirements:
- BCM (Bright Cluster Manager)
- Ubuntu 24.04 APT repositories
- Access to BCM Slurm packages (slurm24.11, slurm25.05)
- cm-wlm-setup tool
- cmsh access

### Healthcheck Requirements:
- Python 3.12+
- Slurm commands: sinfo, squeue, sacct, sacctmgr, scontrol
- System commands: systemctl, munge
- Optional: pdsh (for multi-node operations)

**No external Python packages required** - uses only standard library!

## Testing Recommendations

Before production upgrade:
1. ✓ Test healthcheck in lab environment
2. ✓ Verify baseline capture works correctly
3. ✓ Practice upgrade procedure in lab
4. ✓ Validate post-upgrade comparison catches issues
5. ✓ Test rollback procedures
6. ✓ Document actual time taken for each step

## Support Files Location

All files are in: `/root/slurm-upgrade/`

```
/root/slurm-upgrade/
├── slurm-upgrade-23.11-to-25.05-plan.md   # Upgrade procedure
├── slurm-healthcheck-plan.md               # Healthcheck design
├── slurm-healthcheck.py                    # Main tool (executable)
├── healthcheck-config.conf                 # Configuration
├── README-healthcheck.md                   # User guide
└── 00-PROJECT-SUMMARY.md                   # This file
```

## Next Steps

1. **Review the upgrade plan** thoroughly
2. **Test the healthcheck** on current cluster
3. **Capture a baseline** for practice
4. **Schedule maintenance windows** (two 2-3 hour windows)
5. **Notify users** well in advance
6. **Execute upgrade** following the documented procedure
7. **Validate with healthcheck** after each upgrade phase

## Version Information

- **Created**: November 19, 2025
- **Python Version**: 3.12.3
- **Target Slurm Versions**: 23.11 → 24.11 → 25.05
- **Platform**: Ubuntu 24.04, BCM-managed cluster
- **Cluster Size**: 31 compute nodes

## Contact

For questions or issues during the upgrade, refer to:
- Admin manual: `.docs/admin-manual.txt`
- CBU documentation: `.docs/CBU_Upgrade.txt`
- This project documentation

---

**Good luck with your upgrade!** 🚀

