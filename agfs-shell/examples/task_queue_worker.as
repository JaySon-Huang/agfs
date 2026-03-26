#!/usr/bin/env agfs

# Task Queue Worker - Process tasks from QueueFS in a loop
#
# Usage:
#   ./task_queue_worker.as [queue_path]
#
# Example:
#   ./task_queue_worker.as /queue/mem/task_queue

# =============================================================================
# Configuration
# =============================================================================

# Queue path (can be overridden via argument)
if [ -n "$1" ]; then
    QUEUE_PATH=$1
else
    QUEUE_PATH=/queue/mem/task_queue
fi

# Queue operation file paths
DEQUEUE_FILE=$QUEUE_PATH/dequeue
SIZE_FILE=$QUEUE_PATH/size
ACK_FILE=$QUEUE_PATH/ack
NACK_FILE=$QUEUE_PATH/nack

# Poll interval in seconds
POLL_INTERVAL=2

echo "=========================================="
echo "  Task Queue Worker"
echo "=========================================="
echo "Queue Path: $QUEUE_PATH"
echo "=========================================="
echo ""

# Initialize queue
echo "Initializing queue..."
mkdir $QUEUE_PATH

# Task counter
task_count=0

# Main loop
while true; do
    # Get queue size
    size=$(cat $SIZE_FILE)

    if [ "$size" = "0" ]; then
        echo "Queue empty, waiting ${POLL_INTERVAL}s..."
        sleep $POLL_INTERVAL
        continue
    fi

    if [ -z "$size" ]; then
        echo "Queue empty, waiting ${POLL_INTERVAL}s..."
        sleep $POLL_INTERVAL
        continue
    fi

    echo "Queue size: $size"

    # Dequeue task
    task_json=$(cat $DEQUEUE_FILE)

    if [ -z "$task_json" ]; then
        continue
    fi

    task_count=$((task_count + 1))

    echo ""
    echo "=========================================="
    echo "Task #$task_count received"
    echo "=========================================="

    # Print raw JSON
    echo "Raw: $task_json"
    echo "----------------------------------------"

    task_id=$(echo "$task_json" | jq -r '.id')

    # ==========================================================
    # Add your task processing logic here
    # You can use $task_json to inspect payload, resource_id, etc.
    # ==========================================================
    echo "Processing task #$task_count..."

    if [ -z "$task_id" ]; then
        echo "Missing task id, skipping"
        continue
    fi

    sleep 1

    # Ack only after the durable success condition has been reached.
    echo "{\"id\":\"$task_id\"}" > $ACK_FILE
    echo "Task completed and acked!"

    echo "=========================================="
    echo ""
done
