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
  -n SIZE      Number of files to process in each hsm_restore command (default: 5)
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
  -n SIZE      Number of files to process in each hsm_release command (default: 5)
  -d DIR       Directory to process (required)
```

### 3. s3_prefix_balancer.sh

A utility script for optimizing S3 data distribution by copying files from an existing prefix to a new balanced prefix structure, improving performance when loading data to Lustre via HSM.

#### Features
- Analyzes current S3 object distribution
- Creates a balanced distribution plan across multiple prefixes
- Copies objects to a new prefix structure with even distribution
- Supports optional deletion of source files after copying
- Multi-threaded operation for efficient processing
- Real-time progress monitoring and reporting
- Verifies the new distribution after completion

#### Usage
```bash
./s3_prefix_balancer.sh [-b] [-j JOBS] [-s BATCH_SIZE] [-a] [-d] [-n PREFIX_COUNT] -B BUCKET -p SOURCE_PREFIX [-t TARGET_PREFIX]

Options:
  -b           Run in background mode (using nohup)
  -j JOBS      Number of parallel jobs (default: 32)
  -s SIZE      Batch size for progress reporting (default: 10000)
  -B BUCKET    S3 bucket name (required)
  -p PREFIX    Source S3 prefix to copy from (required)
  -t PREFIX    Target prefix to copy to (default: balance_prefix)
  -a           Analyze only, don't perform balancing
  -d           Delete source files after copying (default: false)
  -n COUNT     Target number of prefixes (default: auto-determine)
```
