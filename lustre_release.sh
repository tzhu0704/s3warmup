#!/bin/bash

# Configuration
LOG_DIR="."
LOG_FILE="${LOG_DIR}/lustre_release_$(date +%Y%m%d_%H%M%S).log"
PARALLEL_JOBS=32  # Number of parallel restore jobs
BACKGROUND=false
BATCH_SIZE=10000  # Process files in batches for progress reporting
HSM_RELEASE_BATCH=5  # Number of files to process in each hsm_release command

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to format time
format_time() {
    local seconds=$1
    local hours=$((seconds / 3600))
    local minutes=$(( (seconds % 3600) / 60 ))
    local remaining_seconds=$((seconds % 60))
    printf "%02d:%02d:%02d" $hours $minutes $remaining_seconds
}

# Function to show usage
usage() {
    echo "Usage: $0 [-b] [-j JOBS] [-s BATCH_SIZE] [-n HSM_BATCH] -d DIRECTORY"
    echo "  -b           Run in background mode (nohup)"
    echo "  -j JOBS      Number of parallel jobs (default: 32)"
    echo "  -s SIZE      Batch size for progress reporting (default: 10000)"
    echo "  -n SIZE      Number of files to process in each hsm_release command (default: 5)"
    echo "  -d DIR       Directory to process (required)"
    exit 1
}

# Parse command line arguments
DIRECTORY=""
while getopts "bd:j:s:n:h" opt; do
    case $opt in
        b) BACKGROUND=true ;;
        d) DIRECTORY=$OPTARG ;;
        j) PARALLEL_JOBS=$OPTARG ;;
        s) BATCH_SIZE=$OPTARG ;;
        n) HSM_RELEASE_BATCH=$OPTARG ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Check if directory is provided
if [ -z "$DIRECTORY" ]; then
    usage
fi

# If background mode is enabled, restart the script with nohup
if [ "$BACKGROUND" = true ] && [ -z "$NOHUP_ACTIVE" ]; then
    log "Restarting in background mode..."
    export NOHUP_ACTIVE=1
    nohup "$0" "$@" > "${LOG_FILE}_nohup" 2>&1 &
    echo "Process started in background with PID $!"
    echo "You can monitor progress with: tail -f ${LOG_FILE}_nohup"
    echo "Or check the log file: $LOG_FILE"
    exit 0
fi

# Main process
log "Starting Lustre release process for directory: $DIRECTORY"
log "Using $PARALLEL_JOBS parallel jobs"
log "Using batch size of $HSM_RELEASE_BATCH files per hsm_release command"
START_TIME=$(date +%s)

# Create temporary directory for processing
TEMP_DIR=$(mktemp -d)
TEMP_ALL_FILES="$TEMP_DIR/all_files.txt"
TEMP_WARMUP_FILES="$TEMP_DIR/warmup_files.txt"
TEMP_SUCCESS="$TEMP_DIR/success.txt"
TEMP_FAILED="$TEMP_DIR/failed.txt"
PROGRESS_FILE="$TEMP_DIR/progress.txt"

# Create empty files
touch "$TEMP_ALL_FILES" "$TEMP_WARMUP_FILES" "$TEMP_SUCCESS" "$TEMP_FAILED"
echo "0" > "$PROGRESS_FILE"

log "Scanning for files..."
# Use find with progress reporting for large directories
total_files=0
find "$DIRECTORY" -type f | while read -r file; do
    echo "$file" >> "$TEMP_ALL_FILES"
    ((total_files++))
    
    # Show progress every BATCH_SIZE files
    if [ $((total_files % BATCH_SIZE)) -eq 0 ]; then
        log "Scanning progress: $total_files files found so far..."
    fi
done

TOTAL_FILES=$(wc -l < "$TEMP_ALL_FILES")
log "Found total $TOTAL_FILES files"

# Find files that need release - using parallel processing
log "Identifying release files in parallel..."

# Create a named pipe for collecting results
FIFO_IDENTIFY="$TEMP_DIR/identify_fifo"
mkfifo "$FIFO_IDENTIFY"
log "$TEMP_ALL_FILES"
# Start background process to collect results
(
    processed=0
    while IFS= read -r file; do
        if [[ -n "$file" ]]; then
            echo "$file" >> "$TEMP_WARMUP_FILES"
        fi
        
        ((processed++))
        if [ $((processed % BATCH_SIZE)) -eq 0 ]; then
            log "Checking files: $processed/$TOTAL_FILES ($(( processed * 100 / TOTAL_FILES ))%)"
        fi
    done < "$FIFO_IDENTIFY"
) &
COLLECTOR_PID=$!

# Process files in parallel to identify which need release
cat "$TEMP_ALL_FILES" | xargs -P "$PARALLEL_JOBS" -I{} bash -c '
    file="$1"
    if lfs hsm_state "$file" 2>/dev/null | grep -q "exists archived" && ! lfs hsm_state "$file" 2>/dev/null | grep -q "released exists archived"; then
        echo "$file"
    fi
' -- {} > "$FIFO_IDENTIFY"

# Wait for collector to finish
wait $COLLECTOR_PID

WARMUPED_FILES=$(wc -l < "$TEMP_WARMUP_FILES")
log "Found $WARMUPED_FILES files that need to release"

if [ "$WARMUPED_FILES" -eq 0 ]; then
    log "No files need to release. Exiting."
    rm -rf "$TEMP_DIR"
    exit 0
fi

# Process files in parallel using xargs
log "Starting release process with $PARALLEL_JOBS parallel jobs"
log "Processing $HSM_RELEASE_BATCH files per hsm_release command"

# Create a named pipe for real-time progress monitoring
PROGRESS_PIPE="$TEMP_DIR/progress_pipe"
mkfifo "$PROGRESS_PIPE"

# Start background process to monitor progress
(
    while IFS= read -r line; do
        if [[ $line == SUCCESS* ]]; then
            echo "${line#SUCCESS }" >> "$TEMP_SUCCESS"
        elif [[ $line == FAILED* ]]; then
            echo "${line#FAILED }" >> "$TEMP_FAILED"
        fi

        # Calculate current progress
        SUCCESS=$(wc -l < "$TEMP_SUCCESS")
        FAILED=$(wc -l < "$TEMP_FAILED")
        PROCESSED=$((SUCCESS + FAILED))

        if [ $((PROCESSED % 1000)) -eq 0 ] || [ "$PROCESSED" -eq "$WARMUPED_FILES" ]; then
            CURRENT_TIME=$(date +%s)
            ELAPSED_SO_FAR=$((CURRENT_TIME - START_TIME))
            if [ $ELAPSED_SO_FAR -gt 0 ]; then
                RATE=$(bc <<< "scale=2; $PROCESSED / $ELAPSED_SO_FAR")
            else
                RATE="N/A"
            fi
            
            PROGRESS=$((PROCESSED * 100 / WARMUPED_FILES))
            log "Progress: $PROGRESS% ($PROCESSED/$WARMUPED_FILES) - Rate: $RATE files/sec - Success: $SUCCESS - Failed: $FAILED"
        fi
    done < "$PROGRESS_PIPE"
) &
MONITOR_PID=$!

# Process files and send output to the pipe
# Use split processing for very large file lists to avoid command line length limits
if [ "$WARMUPED_FILES" -gt 100000 ]; then
    log "Large file count detected, processing in batches..."
    
    # Split the file list into smaller chunks
    split -l 10000 "$TEMP_WARMUP_FILES" "$TEMP_DIR/batch_"
    
    # Process each batch
    for batch_file in "$TEMP_DIR"/batch_*; do
        cat "$batch_file" | xargs -P "$PARALLEL_JOBS" -n "$HSM_RELEASE_BATCH" bash -c '
            files=("$@")
            if sudo lfs hsm_release "${files[@]}" 2>/dev/null; then
                for file in "${files[@]}"; do
                    echo "SUCCESS $file"
                done
            else
                for file in "${files[@]}"; do
                    echo "FAILED $file"
                done
            fi
        ' bash >> "$PROGRESS_PIPE"
    done
else
    # Process all files at once for smaller lists
    cat "$TEMP_WARMUP_FILES" | xargs -P "$PARALLEL_JOBS" -n "$HSM_RELEASE_BATCH" bash -c '
        files=("$@")
        if sudo lfs hsm_release "${files[@]}" 2>/dev/null; then
            for file in "${files[@]}"; do
                echo "SUCCESS $file"
            done
        else
            for file in "${files[@]}"; do
                echo "FAILED $file"
            done
        fi
    ' bash > "$PROGRESS_PIPE"
fi

# Close the pipe to signal the monitor that we're done
exec {PROGRESS_PIPE}>&-

# Wait for monitor process to finish
wait $MONITOR_PID

# Calculate final statistics
END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))
FORMATTED_TIME=$(format_time $TOTAL_TIME)

# Get final counts
FINAL_SUCCESS=$(wc -l < "$TEMP_SUCCESS")
FINAL_FAILED=$(wc -l < "$TEMP_FAILED")
FINAL_PROCESSED=$((FINAL_SUCCESS + FINAL_FAILED))

# Final report
log "Release process completed"
log "----------------------------------------"
log "Total time: $FORMATTED_TIME"
log "Total files processed: $FINAL_PROCESSED"
log "Successfully released: $FINAL_SUCCESS"
log "Failed to release: $FINAL_FAILED"

# Copy results to permanent log files
if [ $FINAL_FAILED -gt 0 ]; then
    cp "$TEMP_FAILED" "${LOG_FILE}_failed"
    log "Failed files are listed in: ${LOG_FILE}_failed"
fi
if [ $FINAL_SUCCESS -gt 0 ]; then
    cp "$TEMP_SUCCESS" "${LOG_FILE}_success"
    log "Successful files are listed in: ${LOG_FILE}_success"
fi
log "----------------------------------------"

# Cleanup
rm -rf "$TEMP_DIR"

# Create completion marker if running in background
if [ "$BACKGROUND" = true ]; then
    touch "${LOG_FILE}.completed"
    log "Background job completed. Marker file created: ${LOG_FILE}.completed"
fi


