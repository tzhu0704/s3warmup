# Lustre HSM Management Scripts

This repository contains scripts for managing Lustre HSM (Hierarchical Storage Management) data migration between Lustre filesystem and S3.

## Scripts Overview

### 1. lustre_warmup.sh

A utility script for warming up Lustre HSM archived data from S3 back to Lustre filesystem.

#### Features
- Supports intermittent data restoration
- Checks HSM state of files in the specified directory
- Builds and processes warmup lists efficiently
- Multi-threaded operation support
- Background execution mode
- Real-time progress monitoring
- Configurable batch size for progress reporting

#### Usage
```bash
./lustre_warmup.sh [-b] [-j JOBS] [-s BATCH_SIZE] -d DIRECTORY

Options:
  -b           Run in background mode (using nohup)
  -j JOBS      Number of parallel jobs (default: 32)
  -s SIZE      Batch size for progress reporting (default: 10000)
  -d DIR       Directory to process (required)
```

### 2. lustre_release.sh
A utility script for releasing Lustre files to S3 storage, freeing up local storage space while maintaining data accessibility through HSM.
#### Features
- Manages file release operations to S3
- Verifies file status before release
- Supports batch processing
- Progress monitoring capabilities

#### Usage

```bash
./lustre_release.sh [-b] [-j JOBS] [-s BATCH_SIZE] -d DIRECTORY

Options:
  -b           Run in background mode (using nohup)
  -j JOBS      Number of parallel jobs (default: 32)
  -s SIZE      Batch size for progress reporting (default: 10000)
  -d DIR       Directory to process (required)
```