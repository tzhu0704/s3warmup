# Lustre HSM Management Scripts

This repository contains scripts for managing Lustre HSM (Hierarchical Storage Management) data migration between Lustre filesystem and S3.

## Scripts Overview

### 0. s3_analyze.sh

A utility script for analyzing S3 bucket directory structure and providing Lustre striping recommendations.

#### Features
- Analyzes S3 bucket directory structure at specified depth
- Identifies large directories that may benefit from Lustre striping
- Provides specific striping recommendations based on analysis
- Supports background execution mode
- Generates detailed reports with statistics

#### Usage
```bash
./s3_analyze.sh [-b] <bucket> <prefix> <depth> [sample_size]

Options:
  -b           Run in background mode (using nohup)
  bucket       S3 bucket name
  prefix       Prefix path to analyze, use '/' for root directory
  depth        Recursion depth for analysis
  sample_size  Number of objects to sample per directory (default: 1000)
```

#### Example
```bash
# Analyze bucket with default settings
./s3_analyze.sh my-bucket / 3

# Run analysis in background mode
./s3_analyze.sh -b my-bucket /data 2

# Analyze with larger sample size
./s3_analyze.sh my-bucket /data 3 5000
```

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
./lustre_warmup.sh [-b] [-j JOBS] [-s BATCH_SIZE] [-n RESTORE_BATCH] -d DIRECTORY

Options:
  -b           Run in background mode (using nohup)
  -j JOBS      Number of parallel jobs (default: 32)
  -s SIZE      Batch size for progress reporting (default: 10000)
  -n SIZE      Number of files to process in each hsm_restore command (default: 5)
  -d DIR       Directory to process (required)
```

#### Example
```bash
# Run warmup with 64 parallel jobs
./lustre_warmup.sh -j 64 -d /lustre/mydata

# Run in background mode with default settings
./lustre_warmup.sh -b -d /lustre/mydata

# Process 10 files per restore command
./lustre_warmup.sh -n 10 -d /lustre/mydata
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
./lustre_release.sh [-b] [-j JOBS] [-s BATCH_SIZE] [-n RELEASE_BATCH] -d DIRECTORY

Options:
  -b           Run in background mode (using nohup)
  -j JOBS      Number of parallel jobs (default: 32)
  -s SIZE      Batch size for progress reporting (default: 10000)
  -n SIZE      Number of files to process in each hsm_release command (default: 5)
  -d DIR       Directory to process (required)
```

#### Example
```bash
# Release files with 16 parallel jobs
./lustre_release.sh -j 16 -d /lustre/mydata

# Run release in background mode
./lustre_release.sh -b -d /lustre/mydata
```

### 3. lustre_archive.sh

A utility script for archiving Lustre files to S3 storage, ensuring data is safely stored in the HSM backend.

#### Features
- Identifies files that need to be archived
- Manages file archive operations to S3
- Supports batch processing for efficiency
- Multi-threaded operation support
- Background execution mode
- Real-time progress monitoring
- Configurable batch size for progress reporting

#### Usage
```bash
./lustre_archive.sh [-b] [-j JOBS] [-s BATCH_SIZE] [-n ARCHIVE_BATCH] -d DIRECTORY

Options:
  -b           Run in background mode (using nohup)
  -j JOBS      Number of parallel jobs (default: 32)
  -s SIZE      Batch size for progress reporting (default: 10000)
  -n SIZE      Number of files per archive command (default: 5)
  -d DIR       Directory to process (required)
```

#### Example
```bash
# Archive files with 10 parallel jobs
./lustre_archive.sh -j 10 -d /mnt/lustre/data

# Run archive in background mode with custom batch size
./lustre_archive.sh -b -s 1000 -d /mnt/lustre/data

# Archive with 20 files per archive command
./lustre_archive.sh -n 20 -d /mnt/lustre/data
```

#### Note
The archive operation may require sudo privileges. If you encounter permission errors, try running the script with sudo:
```bash
sudo ./lustre_archive.sh -d /mnt/lustre/data
```

### 4. s3_prefix_balancer.sh

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

## Prerequisites
- Lustre filesystem configured with HSM
- S3 storage backend properly configured
- Appropriate permissions to execute HSM operations

## Notes
- Ensure sufficient S3 and network bandwidth for optimal performance
- Monitor system resources when running with high parallel job counts
- For large directories, consider using background mode
- Check Lustre logs for detailed operation status

## Best Practices
1. Start with a smaller number of parallel jobs and adjust based on system performance
2. Use batch size appropriate for your dataset size
3. Always verify the target directory before running operations
4. Monitor system resources during large operations
5. Keep regular backups before performing bulk operations