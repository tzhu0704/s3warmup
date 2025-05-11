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

# Create temporary files
ARCHIVE_LIST=$(mktemp)
PROGRESS_FILE=$(mktemp)
trap 'rm -f "$ARCHIVE_LIST" "$PROGRESS_FILE"' EXIT

# Function to build archive list
build_archive_list() {
    local dir="$1"
    echo "Finding files in $dir that need archiving..."
    
    # Find files that are not already archived
    find "$dir" -type f -exec lfs hsm_state {} \; 2>/dev/null | \
        grep -v "exists archived" | \
        awk '{print $1}' | sed 's/:$//' > "$ARCHIVE_LIST"
    
    # Debug: Show what we found
    local found=$(wc -l < "$ARCHIVE_LIST")
    echo "Found $found files that need archiving"
    
    # Debug: Show the first few files
    if [ "$found" -gt 0 ]; then
        echo "First few files to archive:"
        head -n 5 "$ARCHIVE_LIST"
    else
        echo "Warning: No files found for archiving"
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

    # Process files in batches
    local processed=0
    while read -r file; do
        # Collect a batch of files
        batch_files=()
        for ((i=0; i<ARCHIVE_BATCH; i++)); do
            if [ -n "$file" ] && [ -f "$file" ]; then
                batch_files+=("$file")
            else
                echo "Warning: File not found or not accessible: $file"
            fi
            
            # Read the next file, break if no more files
            if ! read -r file; then
                break
            fi
        done
        
        # Process the batch if not empty
        if [ ${#batch_files[@]} -gt 0 ]; then
            if sudo lfs hsm_archive "${batch_files[@]}" 2>/dev/null; then
                processed=$((processed + ${#batch_files[@]}))
                echo "$processed" > "$PROGRESS_FILE"
                
                # Show progress at regular intervals
                if [ $((processed % BATCH_SIZE)) -eq 0 ] || [ "$processed" -eq "$total_files" ]; then
                    percent=$((processed * 100 / total_files))
                    echo "Progress: $processed / $total_files files ($percent%)"
                fi
            else
                echo "Error archiving batch starting with ${batch_files[0]}"
                processed=$((processed + ${#batch_files[@]}))
                echo "$processed" > "$PROGRESS_FILE"
            fi
            
            # Control the number of parallel jobs
            while [ "$(jobs -r | wc -l)" -ge "$JOBS" ]; do
                sleep 0.5
            done
        fi
    done < "$ARCHIVE_LIST"

    # Wait for all background jobs to complete
    wait

    # Final progress report
    echo "Archive process completed: $processed / $total_files files"
}

# Main execution
echo "Building archive list for directory: $DIRECTORY"
build_archive_list "$DIRECTORY"

if [ "$BACKGROUND" = true ]; then
    echo "Running in background mode"
    LOG_FILE="archive_$(date +%Y%m%d_%H%M%S).log"
    
    # Run the same script in background mode
    nohup "$0" -j "$JOBS" -s "$BATCH_SIZE" -n "$ARCHIVE_BATCH" -d "$DIRECTORY" > "$LOG_FILE" 2>&1 &
    
    echo "Process started in background. Check $LOG_FILE for progress"
    exit 0
else
    process_files
fi