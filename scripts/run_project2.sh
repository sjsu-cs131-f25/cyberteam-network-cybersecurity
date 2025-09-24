#!/bin/bash
# run_project2.sh
# Usage: ./scripts/run_project2.sh
# Generates 1k sample from Wednesday-workingHours.pcap_ISCX.csv

set -e  # stop on error

echo "Generating 1k sample..."
mkdir -p data/samples
head -n 1 Wednesday-workingHours.pcap_ISCX.csv > data/samples/sample1k.csv
tail -n +2 Wednesday-workingHours.pcap_ISCX.csv | shuf -n 1000 >> data/samples/sample1k.csv
echo "Sample created at data/samples/sample1k.csv"
