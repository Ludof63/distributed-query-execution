#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $(basename "$0") <URL://filelist.csv>"
  exit 1
fi

# Monitor the IO read activity of a command
traceIO() {
  touch "$1"
  TRACE="$1" "${@:2}"
}

amountOfWorker=4

# Spawn the coordinator process
traceIO trace0.txt \
  cmake-build-debug/coordinator "$1" 4242 &

# Spawn some workers
for i in $(seq 1 $amountOfWorker); do
  traceIO "trace$i.txt" \
    cmake-build-debug/worker "localhost" "4242" &
done

# And wait for completion
time wait
echo ""

# Then check how much work each worker did
for i in $(seq 1 $amountOfWorker); do
  # We need to download 100 files. While we might get some skew, each of our
  # four workers should do about 20 of them. But definitely more than 10.
  downloads=$(wc -l <"trace$i.txt")
  average=$(expr 100 / $amountOfWorker)
  echo "Worker $i has done $downloads. Average is $average."
done


for i in $(seq 0 $amountOfWorker); do rm "trace$i.txt"; done