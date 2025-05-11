#!/bin/bash

# Default values
JOBS=32
BATCH_SIZE=10000
ARCHIVE_BATCH=5  # Default number of files per archive command
BACKGROUND=false
DIRECTORY=""

# Usage function
usage() {
    echo "Usage: $0 [-b] [-j JOBS] [-s BATCH_SIZE] [-n ARCHIVE_BATCH] -d DIRECTORY"
    echo "  -b           Run in background"
    echo "  -j JOBS      Number of parallel jobs (default: 32)"
    echo "  -s SIZE      Batch size for progress reporting (default: 10000)"
    echo "  -n SIZE      Number of files per archive command (default: 5)"
    echo "  -d DIR       Directory to process (required)"
    exit 1
}

# Parse command line options
while getopts "bj:s:n:d:" opt; do
    case $opt in
        b) BACKGROUND=true ;;
        j) JOBS="$OPTARG" ;;
        s) BATCH_SIZE="$OPTARG" ;;
        n) ARCHIVE_BATCH="$OPTARG" ;;
        d) DIRECTORY="$OPTARG" ;;
        *) usage ;;
    esac
done

# Validate parameters
if [ -z "$DIRECTORY" ]; then
    echo "Error: Directory parameter (-d) is required"
    usage
fi

# Ensure numeric parameters have valid values
if [ -z "$JOBS" ] || ! [[ "$JOBS" =~ ^[0-9]+$ ]]; then
    echo "Error: Invalid number of jobs, using default: 32"
    JOBS=32
elif [ "$JOBS" -lt 1 ]; then
    echo "Error: Number of jobs must be at least 1, using default: 32"
    JOBS=32
fi

if [ -z "$BATCH_SIZE" ] || ! [[ "$BATCH_SIZE" =~ ^[0-9]+$ ]]; then
    echo "Error: Invalid batch size, using default: 10000"
    BATCH_SIZE=10000
elif [ "$BATCH_SIZE" -lt 1 ]; then
    echo "Error: Batch size must be at least 1, using default: 10000"
    BATCH_SIZE=10000
fi

if [ -z "$ARCHIVE_BATCH" ] || ! [[ "$ARCHIVE_BATCH" =~ ^[0-9]+$ ]]; then
    echo "Error: Invalid archive batch size, using default: 5"
    ARCHIVE_BATCH=5
elif [ "$ARCHIVE_BATCH" -lt 1 ]; then
    echo "Error: Archive batch size must be at least 1, using default: 5"
    ARCHIVE_BATCH=5
fi

# Create temporary files
ARCHIVE_LIST=$(mktemp)
PROGRESS_FILE=$(mktemp)
trap 'rm -f "$ARCHIVE_LIST" "$PROGRESS_FILE"' EXIT

# Function to build archive list
build_archive_list() {
    local dir="$1"
    echo "Finding files in $dir that need archiving..."
    find "$dir" -type f -exec lfs hsm_state {} \; 2>/dev/null | \
        grep -v "exists archived" | \
        awk '{print $1}' > "$ARCHIVE_LIST"
    
    # Check if the list is empty or has errors
    if [ ! -s "$ARCHIVE_LIST" ]; then
        echo "Warning: No files found for archiving or error accessing files"
    fi
}

# Function to update progress
update_progress() {
    local increment=$1
    local current=0
    
    if [ -f "$PROGRESS_FILE" ]; then
        current=$(cat "$PROGRESS_FILE" 2>/dev/null || echo "0")
        # Ensure current is a number
        if ! [[ "$current" =~ ^[0-9]+$ ]]; then
            current=0
        fi
    fi
    
    echo $((current + increment)) > "$PROGRESS_FILE"
}

# Function to archive a batch of files
archive_batch() {
    local files=("$@")
    if [ ${#files[@]} -gt 0 ]; then
        if sudo lfs hsm_archive "${files[@]}" 2>/dev/null; then
            update_progress ${#files[@]}
        else
            echo "Error archiving batch starting with ${files[0]}" >&2
            # Still update progress to avoid getting stuck
            update_progress ${#files[@]}
        fi
    fi
}

# Function to process files
process_files() {
    local total_files=0
    
    if [ -f "$ARCHIVE_LIST" ]; then
        total_files=$(wc -l < "$ARCHIVE_LIST" 2>/dev/null || echo "0")
        # Ensure total_files is a number
        if ! [[ "$total_files" =~ ^[0-9]+$ ]]; then
            total_files=0
        fi
    fi
    
    echo 0 > "$PROGRESS_FILE"  # Initialize progress counter
    
    if [ "$total_files" -eq 0 ]; then
        echo "No files need to be archived"
        return 0
    fi

    echo "Starting archive of $total_files files..."
    echo "Using $JOBS parallel jobs with $ARCHIVE_BATCH files per archive command"

    # Start progress monitoring in background
    (
        while true; do
            if [ ! -f "$PROGRESS_FILE" ]; then
                echo "Progress file not found, monitoring stopped"
                break
            fi
            
            processed=$(cat "$PROGRESS_FILE" 2>/dev/null || echo "0")
            # Ensure processed is a number
            if ! [[ "$processed" =~ ^[0-9]+$ ]]; then
                processed=0
            fi
            
            if [ "$processed" -gt 0 ] && [ "$((processed % BATCH_SIZE))" -eq 0 ]; then
                if [ "$total_files" -gt 0 ]; then
                    percent=$((processed * 100 / total_files))
                    echo "Progress: $processed / $total_files files ($percent%)"
                else
                    echo "Progress: $processed files"
                fi
            fi
            
            if [ "$processed" -ge "$total_files" ] && [ "$total_files" -gt 0 ]; then
                echo "Completed: $total_files / $total_files files (100%)"
                break
            fi
            
            sleep 5
        done
    ) &
    MONITOR_PID=$!

    # Process files
    local processed=0
    while [ "$processed" -lt "$total_files" ]; do
        batch_files=()
        for ((i=0; i<ARCHIVE_BATCH; i++)); do
            if [ "$processed" -ge "$total_files" ]; then
                break
            fi
            
            file=$(sed -n "$((processed + 1))p" "$ARCHIVE_LIST" 2>/dev/null)
            if [ -n "$file" ] && [ -f "$file" ]; then
                batch_files+=("$file")
                processed=$((processed + 1))
            else
                # Skip invalid files but count them as processed
                processed=$((processed + 1))
                continue
            fi
        done
        
        if [ ${#batch_files[@]} -gt 0 ]; then
            archive_batch "${batch_files[@]}" &
            
            # Control the number of parallel jobs
            while [ "$(jobs -r | wc -l)" -ge "$JOBS" ]; do
                sleep 0.5
            done
        fi
    done

    # Wait for all background jobs to complete
    wait

    # Kill the progress monitor
    if [ -n "$MONITOR_PID" ]; then
        kill $MONITOR_PID 2>/dev/null || true
        wait $MONITOR_PID 2>/dev/null || true
    fi

    # Final progress report
    echo "Archive process completed: $total_files files processed"
}

# Main execution
echo "Building archive list for directory: $DIRECTORY"
build_archive_list "$DIRECTORY"

if [ "$BACKGROUND" = true ]; then
    echo "Running in background mode"
    LOG_FILE="archive_$(date +%Y%m%d_%H%M%S).log"
    nohup bash -c "$(declare -f update_progress archive_batch process_files build_archive_list); PROGRESS_FILE='$PROGRESS_FILE' ARCHIVE_LIST='$ARCHIVE_LIST' BATCH_SIZE=$BATCH_SIZE JOBS=$JOBS ARCHIVE_BATCH=$ARCHIVE_BATCH process_files" > "$LOG_FILE" 2>&1 &
    echo "Process started in background. Check $LOG_FILE for progress"
else
    process_files
fi