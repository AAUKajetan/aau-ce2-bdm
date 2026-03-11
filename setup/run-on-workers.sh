#!/bin/bash
# Usage: ./run-on-workers.sh "command to run on all nodes"
# Example: ./run-on-workers.sh "sudo apt install -y openjdk-17-jdk"

NODES=(hadoop-worker1 hadoop-worker2 hadoop-worker3 hadoop-worker4)
SUDO_PASS="hadoop"
CMD="$*"

if [ -z "$CMD" ]; then
  echo "Usage: $0 \"command\""
  exit 1
fi

echo "Running: $CMD"
echo "---"

for host in "${NODES[@]}"; do
  echo "=== $host ==="
  if [[ "$CMD" == sudo* ]]; then
    ssh "hadoop@$host" "cd /home/hadoop && echo '$SUDO_PASS' | sudo -S bash -c '${CMD#sudo }'"
  else
    ssh "hadoop@$host" "cd /home/hadoop && $CMD"
  fi
done

echo "---"
echo "Done"
