#!/bin/bash

# S3 Prefix Balancer
# This script copies files from an existing S3 prefix to a new balanced prefix structure
# to improve performance when loading data to Lustre via HSM

# Configuration
LOG_DIR="."
LOG_FILE="${LOG_DIR}/s3_prefix_balancer_$(date +%Y%m%d_%H%M%S).log"
PARALLEL_JOBS=32  # Number of parallel jobs
BACKGROUND=false
BATCH_SIZE=10000  # Process objects in batches for progress reporting
S3_BUCKET=""      # S3 bucket name
SOURCE_PREFIX=""  # Source S3 prefix to copy from
TARGET_PREFIX="balance_prefix"  # Target prefix to copy to
TARGET_PREFIX_COUNT=0 # Target number of prefixes (0 = auto-determine)
ANALYZE_ONLY=false # Only analyze distribution without balancing
DELETE_SOURCE=false # Whether to delete source files after copying

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
    echo "Usage: $0 [-b] [-j JOBS] [-s BATCH_SIZE] [-a] [-d] [-n PREFIX_COUNT] -B BUCKET -p SOURCE_PREFIX [-t TARGET_PREFIX]"
    echo "  -b           Run in background mode (nohup)"
    echo "  -j JOBS      Number of parallel jobs (default: 32)"
    echo "  -s SIZE      Batch size for progress reporting (default: 10000)"
    echo "  -B BUCKET    S3 bucket name (required)"
    echo "  -p PREFIX    Source S3 prefix to copy from (required)"
    echo "  -t PREFIX    Target prefix to copy to (default: balance_prefix)"
    echo "  -a           Analyze only, don't perform balancing"
    echo "  -d           Delete source files after copying (default: false)"
    echo "  -n COUNT     Target number of prefixes (default: auto-determine)"
    exit 1
}

# Parse command line arguments
while getopts "bj:s:B:p:t:adn:h" opt; do
    case $opt in
        b) BACKGROUND=true ;;
        j) PARALLEL_JOBS=$OPTARG ;;
        s) BATCH_SIZE=$OPTARG ;;
        B) S3_BUCKET=$OPTARG ;;
        p) SOURCE_PREFIX=$OPTARG ;;
        t) TARGET_PREFIX=$OPTARG ;;
        a) ANALYZE_ONLY=true ;;
        d) DELETE_SOURCE=true ;;
        n) TARGET_PREFIX_COUNT=$OPTARG ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Check if required parameters are provided
if [ -z "$S3_BUCKET" ] || [ -z "$SOURCE_PREFIX" ]; then
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
log "Starting S3 prefix balancing"
log "Source: s3://$S3_BUCKET/$SOURCE_PREFIX"
log "Target: s3://$S3_BUCKET/$TARGET_PREFIX"
log "Using $PARALLEL_JOBS parallel jobs"
START_TIME=$(date +%s)

# Create temporary directory for processing
TEMP_DIR=$(mktemp -d)
TEMP_S3_OBJECTS="$TEMP_DIR/s3_objects.txt"
TEMP_BALANCED_PLAN="$TEMP_DIR/balanced_plan.txt"

# Create empty files
touch "$TEMP_S3_OBJECTS" "$TEMP_BALANCED_PLAN"

# Step 1: List all objects in the S3 bucket with the specified prefix
log "Listing objects in S3 bucket: $S3_BUCKET with prefix: $SOURCE_PREFIX"
aws s3 ls "s3://$S3_BUCKET/$SOURCE_PREFIX" --recursive > "$TEMP_S3_OBJECTS"

TOTAL_OBJECTS=$(wc -l < "$TEMP_S3_OBJECTS")
log "Found $TOTAL_OBJECTS objects in S3"

if [ $TOTAL_OBJECTS -eq 0 ]; then
    log "No objects found. Exiting."
    rm -rf "$TEMP_DIR"
    exit 0
fi

# Step 2: Analyze current distribution
log "Analyzing current distribution..."

# Extract object keys and sizes
awk '{print $3 " " $4}' "$TEMP_S3_OBJECTS" > "$TEMP_DIR/objects_with_size.txt"

# Calculate total size
TOTAL_SIZE=$(awk '{sum += $1} END {print sum}' "$TEMP_DIR/objects_with_size.txt")
log "Total size of all objects: $TOTAL_SIZE bytes"

# Determine optimal prefix count if not specified
if [ $TARGET_PREFIX_COUNT -eq 0 ]; then
    # Auto-determine based on object count
    # This is a simple heuristic - adjust as needed for your specific use case
    if [ $TOTAL_OBJECTS -lt 10000 ]; then
        TARGET_PREFIX_COUNT=4
    elif [ $TOTAL_OBJECTS -lt 100000 ]; then
        TARGET_PREFIX_COUNT=8
    elif [ $TOTAL_OBJECTS -lt 1000000 ]; then
        TARGET_PREFIX_COUNT=16
    else
        TARGET_PREFIX_COUNT=32
    fi
    log "Auto-determined target prefix count: $TARGET_PREFIX_COUNT"
else
    log "Using specified target prefix count: $TARGET_PREFIX_COUNT"
fi

if [ "$ANALYZE_ONLY" = true ]; then
    log "Analysis completed. Exiting."
    rm -rf "$TEMP_DIR"
    exit 0
fi

# Step 3: Create a balanced distribution plan
log "Creating balanced distribution plan..."

# Generate new prefix names
for i in $(seq 0 $(($TARGET_PREFIX_COUNT - 1))); do
    # Use zero-padded numbers for better sorting
    printf "prefix%03d\n" $i >> "$TEMP_DIR/new_prefixes.txt"
done

log "Generated $TARGET_PREFIX_COUNT new prefixes"

# Create a plan that distributes objects evenly across prefixes
OBJECT_COUNT=0
PREFIX_INDEX=0

while read -r line; do
    # Parse size and object key
    SIZE=$(echo "$line" | awk '{print $1}')
    OBJECT_KEY=$(echo "$line" | awk '{print $2}')
    
    # Get target prefix
    TARGET_SUB_PREFIX=$(sed -n "$((PREFIX_INDEX + 1))p" "$TEMP_DIR/new_prefixes.txt")
    
    # Extract object name (without source prefix)
    if [[ "$OBJECT_KEY" == "$SOURCE_PREFIX"* ]]; then
        OBJECT_NAME=${OBJECT_KEY#"$SOURCE_PREFIX"}
        # Remove leading slash if present
        OBJECT_NAME=${OBJECT_NAME#/}
    else
        OBJECT_NAME=$OBJECT_KEY
    fi
    
    # Create new object key with target prefix
    NEW_OBJECT_KEY="$TARGET_PREFIX/$TARGET_SUB_PREFIX/$OBJECT_NAME"
    
    # Add to plan
    echo "$OBJECT_KEY,$NEW_OBJECT_KEY" >> "$TEMP_BALANCED_PLAN"
    
    # Update counters
    ((OBJECT_COUNT++))
    PREFIX_INDEX=$(( (PREFIX_INDEX + 1) % TARGET_PREFIX_COUNT ))
    
    # Show progress
    if [ $((OBJECT_COUNT % BATCH_SIZE)) -eq 0 ]; then
        log "  Planned $OBJECT_COUNT/$TOTAL_OBJECTS objects..."
    fi
done < "$TEMP_DIR/objects_with_size.txt"

log "Balanced plan created for $OBJECT_COUNT objects"

# Step 4: Execute the balanced distribution plan
log "Executing balanced distribution plan..."

# Create temporary files for tracking progress
TEMP_SUCCESS="$TEMP_DIR/success.txt"
TEMP_FAILED="$TEMP_DIR/failed.txt"
touch "$TEMP_SUCCESS" "$TEMP_FAILED"

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

        if [ $((PROCESSED % 1000)) -eq 0 ] || [ "$PROCESSED" -eq "$TOTAL_OBJECTS" ]; then
            CURRENT_TIME=$(date +%s)
            ELAPSED_SO_FAR=$((CURRENT_TIME - START_TIME))
            if [ $ELAPSED_SO_FAR -gt 0 ]; then
                RATE=$(bc <<< "scale=2; $PROCESSED / $ELAPSED_SO_FAR")
            else
                RATE="N/A"
            fi
            
            PROGRESS=$((PROCESSED * 100 / TOTAL_OBJECTS))
            log "Progress: $PROGRESS% ($PROCESSED/$TOTAL_OBJECTS) - Rate: $RATE objects/sec - Success: $SUCCESS - Failed: $FAILED"
        fi
    done < "$PROGRESS_PIPE"
) &
MONITOR_PID=$!

# Open the pipe for writing
exec 3>"$PROGRESS_PIPE"

# Process objects in parallel using xargs instead of parallel
# Use split processing for very large object lists to avoid command line length limits
if [ "$TOTAL_OBJECTS" -gt 100000 ]; then
    log "Large object count detected, processing in batches..."
    
    # Split the plan into smaller chunks
    split -l 10000 "$TEMP_BALANCED_PLAN" "$TEMP_DIR/batch_"
    
    # Process each batch
    for batch_file in "$TEMP_DIR"/batch_*; do
        cat "$batch_file" | xargs -P "$PARALLEL_JOBS" -I{} bash -c '
            line="$1"
            src_key=$(echo "$line" | cut -d, -f1)
            dst_key=$(echo "$line" | cut -d, -f2)
            if aws s3 cp "s3://'$S3_BUCKET'/$src_key" "s3://'$S3_BUCKET'/$dst_key"; then
                if [ "'$DELETE_SOURCE'" = "true" ]; then
                    aws s3 rm "s3://'$S3_BUCKET'/$src_key" && echo "SUCCESS $src_key -> $dst_key (deleted source)" || echo "FAILED $src_key (delete failed)"
                else
                    echo "SUCCESS $src_key -> $dst_key"
                fi
            else
                echo "FAILED $src_key -> $dst_key (copy failed)"
            fi
        ' -- {} >> "$PROGRESS_PIPE"
    done
else
    # Process all objects at once for smaller lists
    cat "$TEMP_BALANCED_PLAN" | xargs -P "$PARALLEL_JOBS" -I{} bash -c '
        line="$1"
        src_key=$(echo "$line" | cut -d, -f1)
        dst_key=$(echo "$line" | cut -d, -f2)
        if aws s3 cp "s3://'$S3_BUCKET'/$src_key" "s3://'$S3_BUCKET'/$dst_key"; then
            if [ "'$DELETE_SOURCE'" = "true" ]; then
                aws s3 rm "s3://'$S3_BUCKET'/$src_key" && echo "SUCCESS $src_key -> $dst_key (deleted source)" || echo "FAILED $src_key (delete failed)"
            else
                echo "SUCCESS $src_key -> $dst_key"
            fi
        else
            echo "FAILED $src_key -> $dst_key (copy failed)"
        fi
    ' -- {} > "$PROGRESS_PIPE"
fi

# Close the pipe to signal the monitor that we're done
exec 3>&-

# Wait for monitor process to finish
wait $MONITOR_PID

# Calculate final statistics
END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))
FORMATTED_TIME=$(format_time $TOTAL_TIME)

# Get final counts
FINAL_SUCCESS=$(wc -l < "$TEMP_DIR/success.txt")
FINAL_FAILED=$(wc -l < "$TEMP_DIR/failed.txt")
FINAL_PROCESSED=$((FINAL_SUCCESS + FINAL_FAILED))

# Final report
log "S3 prefix balancing completed"
log "----------------------------------------"
log "Total time: $FORMATTED_TIME"
log "Total objects processed: $FINAL_PROCESSED"
log "Successfully balanced: $FINAL_SUCCESS"
log "Failed to balance: $FINAL_FAILED"

# Copy results to permanent log files
if [ $FINAL_FAILED -gt 0 ]; then
    cp "$TEMP_DIR/failed.txt" "${LOG_FILE}_failed"
    log "Failed objects are listed in: ${LOG_FILE}_failed"
fi
if [ $FINAL_SUCCESS -gt 0 ]; then
    cp "$TEMP_DIR/success.txt" "${LOG_FILE}_success"
    log "Successful objects are listed in: ${LOG_FILE}_success"
fi
log "----------------------------------------"

# Step 5: Verify the new distribution
log "Verifying new prefix distribution..."

# List objects in the target prefix to see the new distribution
TEMP_NEW_OBJECTS="$TEMP_DIR/new_s3_objects.txt"
aws s3 ls "s3://$S3_BUCKET/$TARGET_PREFIX/" --recursive > "$TEMP_NEW_OBJECTS"

NEW_TOTAL_OBJECTS=$(wc -l < "$TEMP_NEW_OBJECTS")
log "Found $NEW_TOTAL_OBJECTS objects in target prefix"

# Extract sub-prefixes and count objects
TEMP_NEW_PREFIX_STATS="$TEMP_DIR/new_prefix_stats.txt"
awk '{print $4}' "$TEMP_NEW_OBJECTS" | awk -F/ '{if (NF>=3) print $2}' | sort | uniq -c | sort -nr > "$TEMP_NEW_PREFIX_STATS"

# Display new prefix distribution
log "New prefix distribution:"
cat "$TEMP_NEW_PREFIX_STATS" | while read -r line; do
    count=$(echo "$line" | awk '{print $1}')
    prefix=$(echo "$line" | awk '{print $2}')
    percentage=$(bc <<< "scale=2; $count * 100 / $NEW_TOTAL_OBJECTS")
    log "  Prefix: $prefix - Count: $count - Percentage: $percentage%"
done

# Calculate statistics for the new distribution
NEW_PREFIX_COUNT=$(wc -l < "$TEMP_NEW_PREFIX_STATS")
if [ $NEW_PREFIX_COUNT -gt 1 ]; then
    NEW_MAX_COUNT=$(head -n 1 "$TEMP_NEW_PREFIX_STATS" | awk '{print $1}')
    NEW_MIN_COUNT=$(tail -n 1 "$TEMP_NEW_PREFIX_STATS" | awk '{print $1}')
    NEW_IMBALANCE_RATIO=$(bc <<< "scale=2; $NEW_MAX_COUNT / $NEW_MIN_COUNT")
    NEW_IMBALANCE_PERCENTAGE=$(bc <<< "scale=2; ($NEW_MAX_COUNT - $NEW_MIN_COUNT) * 100 / $NEW_MAX_COUNT")
    
    log "New distribution statistics:"
    log "  Number of prefixes: $NEW_PREFIX_COUNT"
    log "  Maximum objects in a prefix: $NEW_MAX_COUNT"
    log "  Minimum objects in a prefix: $NEW_MIN_COUNT"
    log "  Imbalance ratio: $NEW_IMBALANCE_RATIO:1"
    log "  Imbalance percentage: $NEW_IMBALANCE_PERCENTAGE%"
else
    log "Warning: Only one prefix found in the new distribution."
fi

log "Balancing operation completed successfully."
log "----------------------------------------"
log "Source: s3://$S3_BUCKET/$SOURCE_PREFIX"
log "Target: s3://$S3_BUCKET/$TARGET_PREFIX"
log "----------------------------------------"

# Cleanup
rm -rf "$TEMP_DIR"

# Create completion marker if running in background
if [ "$BACKGROUND" = true ]; then
    touch "${LOG_FILE}.completed"
    log "Background job completed. Marker file created: ${LOG_FILE}.completed"
fi
