#!/usr/bin/env bash
# run_project2.sh
# Usage: ./run_project2.sh [DATASET_PATH] [DELIM]
# Recreates B (sample), C (shell EDA), and D (logs) for the dataset.
# Defaults: DATASET_PATH = Wednesday-workingHours.pcap_ISCX.csv, DELIM=','

set -euo pipefail

DATASET="${1:-${FILE:-Wednesday-workingHours.pcap_ISCX.csv}}"
DELIM="${2:-,}"

if [[ ! -f "$DATASET" ]]; then
  echo "ERROR: Dataset not found: $DATASET" >&2
  exit 1
fi

mkdir -p out data/samples

# Log everything (stdout to out/run.log, stderr to out/run.err) while still echoing to screen
exec > >(tee -a out/run.log) 2> >(tee -a out/run.err >&2)

echo "# $(date -Is) Starting run on ${DATASET} with DELIM='${DELIM}'"

# Stream helper (supports .gz without full extract)
STREAM="cat"; [[ "$DATASET" == *.gz ]] && STREAM="zcat"

################################################################################
# B) Access & Snapshots (reproducible 1k sample with header preserved)
################################################################################
echo "## [B] Generating 1k sample (header preserved) ..."
$STREAM "$DATASET" | head -n 1 > data/samples/sample1k.csv
$STREAM "$DATASET" | tail -n +2 | shuf -n 1000 >> data/samples/sample1k.csv
echo "Sample: data/samples/sample1k.csv"

################################################################################
# C) Shell-based EDA (frequency tables, Top-N, skinny table, grep demos)
################################################################################
echo "## [C] Building frequency tables, Top-N, and skinny table ..."

# Frequency table — Destination Port (col 1)
$STREAM "$DATASET" | tail -n +2 | cut -d"$DELIM" -f1 \
  | sort | uniq -c | sort -nr \
  | tee out/freq_dst_port.txt | head

# Frequency table — Total Fwd Packets (col 3)
$STREAM "$DATASET" | tail -n +2 | cut -d"$DELIM" -f3 \
  | sort | uniq -c | sort -nr \
  | tee out/freq_total_fwd_pkts.txt | head

# Top-N (Top 10 Destination Ports)
$STREAM "$DATASET" | tail -n +2 | cut -d"$DELIM" -f1 \
  | sort | uniq -c | sort -nr | head -n 10 \
  > out/top_dst_port_10.txt
echo "Wrote: out/top_dst_port_10.txt"

# Skinny table — Destination Port + Total Fwd Packets (cols 1,3), deduped
$STREAM "$DATASET" | tail -n +2 | cut -d"$DELIM" -f1,3 \
  | sort -u > out/skinny_dstport_totalfwdpkts.csv
echo "Wrote: out/skinny_dstport_totalfwdpkts.csv"

# grep demos (case-insensitive, invert match)
$STREAM "$DATASET" | head -n 1 | grep -i 'destination' \
  > out/grep_i_destination_header.txt || true
$STREAM "$DATASET" | tail -n +2 | grep -v '^[[:space:]]*0,' \
  | head -n 5 > out/grep_v_dstport0_preview.txt || true
echo "Wrote: out/grep_i_destination_header.txt, out/grep_v_dstport0_preview.txt"

# Extra tee example for D as well: frequency table for Total Backward Packets (col 4)
$STREAM "$DATASET" | tail -n +2 | cut -d"$DELIM" -f4 \
  | sort | uniq -c | sort -nr \
  | tee out/freq_total_bwd_pkts.txt | head

################################################################################
# D) Logs & Reproducibility (tee already used above; also separate stdout/stderr)
################################################################################
echo "## [D] Demonstrating stdout vs stderr redirection ..."
# Create explicit stdout/stderr example without failing the whole run
( echo "stdout example line"; echo "stderr example line" 1>&2 ) \
  > out/stdout_example.txt 2> out/stderr_example.txt
echo "Wrote: out/stdout_example.txt, out/stderr_example.txt"

# Optional: capture a mini session transcript if 'script' exists
if command -v script >/dev/null 2>&1; then
  echo "## [D] Capturing a short session transcript with 'script' ..."
  script -q -c "$STREAM \"$DATASET\" | tail -n +2 | cut -d\"$DELIM\" -f1 | sort | uniq -c | sort -nr | head -n 5" \
    out/project2_session.txt || true
  echo "Wrote: out/project2_session.txt"
else
  echo "NOTE: 'script' not found; skipping session capture. (Install 'bsdmainutils' or 'util-linux' variant if needed.)" >&2
fi

echo "# $(date -Is) Done. Outputs in ./out and sample in ./data/samples"
