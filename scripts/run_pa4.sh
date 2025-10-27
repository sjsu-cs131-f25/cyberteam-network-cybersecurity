#!/usr/bin/env bash
# Use the .tsv dataset -> "Wednesday-workingHours.pcap_ISCX.tsv" | "sample1k.tsv"

set -euo pipefail

INPUT="${1:-}"
if [ -z "$INPUT" ] || [ ! -f "$INPUT" ]; then
  echo "Usage: bash run_pa4.sh <INPUT>" >&2
  exit 1
fi

mkdir -p out logs
LOG="logs/run_pa4.log"

echo "Running on $INPUT" | tee "$LOG"
# ---------
# --- Task 1: Clean & Normalize ---
# ---------
# Trim extra whitespace and ensure tab-delimited alignment
echo "Cleaning input" | tee -a "$LOG"

# normalize header & rows: trim, collapse internal spaces, keep tabs
sed -E \
  -e 's/\r//g' \
  -e 's/^[[:space:]]+//; s/[[:space:]]+$//' \
  -e 's/[[:space:]]{2,}/\t/g' \
  "$INPUT" > out/cleaned.tsv

# save before/after samples
head -n 5 "$INPUT" > out/sample_before.tsv
head -n 5 out/cleaned.tsv > out/sample_after.tsv

# ---------
# --- Task 2: Frequency + skinny tables ---
# ---------
echo "Building frequency and skinny tables..." | tee -a "$LOG"

# Frequency for Label
awk -F'\t' 'NR>1 {c[$NF]++}
END {
  print "Label\tCount"
  for (k in c) print k "\t" c[k]
}' out/cleaned.tsv | sort -t$'\t' -k2,2nr -k1,1 > out/freq_label.tsv

# Frequency for first column (Destination Port)
awk -F'\t' 'NR>1 {p[$1]++}
END {
  print "Destination_Port\tCount"
  for (k in p) print k "\t" p[k]
}' out/cleaned.tsv | sort -t$'\t' -k2,2nr -k1,1 > out/freq_port.tsv

# Top 10 labels
(head -n1 out/freq_label.tsv && tail -n +2 out/freq_label.tsv | head -n 10) > out/top10_label.tsv

# Skinny table: Destination Port + Label
awk -F'\t' 'NR==1 {print $1 "\t" $NF; next} {print $1 "\t" $NF}' out/cleaned.tsv \
  | sort -t$'\t' -k1,1 -k2,2 > out/skinny.tsv


# ---------
# --- Task 3: AWK quality filters ---
# ---------

echo 'Running AWK quality filters'

awk 'BEGIN {
    FS = OFS = "\t"
    # Create output directory if missing
    system("mkdir -p out")
}

NR == 1 {
    # Print header row unchanged
    print > "out/filtered.tsv"
    next
}

# Define filters for quality
# Adjust field numbers based on your dataset columns
# Example columns from the dataset:
# duration, protocol_type, service, flag, src_bytes, dst_bytes, land, wrong_fragment, urgent, ...

{
    duration = $1
    protocol = $2
    service  = $3
    flag     = $4
    src_b    = $5
    dst_b    = $6

    # --- Quality filters ---
    valid_key = (protocol != "" && service != "" && flag != "")
    positive_counts = (duration >= 0 && src_b >= 0 && dst_b >= 0)
    reasonable_range = (duration <= 1e6 && src_b <= 1e8 && dst_b <= 1e8)
    not_test_row = (tolower($0) !~ /test/)

    if (valid_key && positive_counts && reasonable_range && not_test_row)
        print > "out/filtered.tsv"
}' $INPUT


# ---------
# --- Task 4: AWK Ratios, buckets, and per-entity summaries ---
# ---------

echo 'Calculating AWK ratios, buckets, and per-entity summaries...'

awk 'BEGIN {
    FS = OFS = "\t"
    print "=== Ratio Summary by Service ==="
    print "Service\tCount\tAvgRatio\tMin\tMax"
}

NR == 1 { next }  # skip header

{
    service = $3
    src_b = $5 + 0
    dst_b = $6 + 0

    # --- Compute ratio safely ---
    denom = (dst_b == 0 ? 1 : dst_b)
    ratio = src_b / denom

    # --- Bucketize ratio ---
    if (ratio == 0)
        bucket = "ZERO"
    else if (ratio < 0.5)
        bucket = "LO"
    else if (ratio < 2)
        bucket = "MID"
    else
        bucket = "HI"

    bucket_count[bucket]++

    # --- Per-service aggregation ---
    sum_ratio[service] += ratio
    count[service]++
    if (!(service in min_ratio) || ratio < min_ratio[service]) min_ratio[service] = ratio
    if (!(service in max_ratio) || ratio > max_ratio[service]) max_ratio[service] = ratio
}

END {
    print "\n=== Bucket Counts ==="
    for (b in bucket_count)
        printf "%-6s : %d\n", b, bucket_count[b]

    print "\n=== Per-Service Summary ==="
    for (s in count) {
        avg = sum_ratio[s] / count[s]
        printf "%-20s\t%6d\t%10.3f\t%10.3f\t%10.3f\n", s, count[s], avg, min_ratio[s], max_ratio[s]
    }
}' $INPUT

# ---------
# --- Task 5: String structure ---
# ---------

echo 'Structurizing strings...'


awk 'BEGIN {
    FS = OFS = "\t"
    print "=== String Profiling ==="
}

NR == 1 { next }  # Skip header

{
    # Example field: service or ID-like code such as "ABC-123-XYZ"
    service = $3
    tolower_svc = tolower(service)

    # --- Extract prefix/suffix patterns (before and after '-') ---
    n = split(service, parts, "-")
    prefix = parts[1]
    suffix = parts[n]

    # --- Aggregate prefix/suffix frequencies ---
    prefix_count[prefix]++
    suffix_count[suffix]++

    # --- Normalize for duplicate detection (case-insensitive) ---
    norm = tolower_svc
    norm_count[norm]++

    # --- Compute string length buckets ---
    len = length(service)
    if (len == 0) len_bucket = "EMPTY"
    else if (len < 5) len_bucket = "SHORT"
    else if (len <= 10) len_bucket = "MEDIUM"
    else len_bucket = "LONG"

    len_bucket_count[len_bucket]++
}

END {
    print "\n=== Prefix Frequency (Top) ==="
    for (p in prefix_count)
        if (prefix_count[p] > 5)
            printf "%-10s : %d\n", p, prefix_count[p]

    print "\n=== Suffix Frequency (Top) ==="
    for (s in suffix_count)
        if (suffix_count[s] > 5)
            printf "%-10s : %d\n", s, suffix_count[s]

    print "\n=== Duplicate Cluster Candidates (Case-Insensitive) ==="
    for (n in norm_count)
        if (norm_count[n] > 1)
            printf "%-15s : %d\n", n, norm_count[n]

    print "\n=== Length Bucket Distribution ==="
    for (b in len_bucket_count)
        printf "%-8s : %d\n", b, len_bucket_count[b]
}' $INPUT

# ---------
# --- Task 6: Signal Discovery ---
# ---------

echo 'Discovering numeric signals by Label...' | tee -a "$LOG"

awk -F'\t' -v OFS='\t' '
NR == 1 {
    # Record header mapping for reference
    next
}

{
    label = $NF
    flow_duration = $2 + 0
    fwd_pkts      = $3 + 0
    bwd_pkts      = $4 + 0
    flow_bytes_s  = $15 + 0
    flow_pkts_s   = $16 + 0

    count[label]++
    sum_dur[label] += flow_duration
    sumsq_dur[label] += flow_duration^2

    sum_fwd[label] += fwd_pkts
    sumsq_fwd[label] += fwd_pkts^2

    sum_bwd[label] += bwd_pkts
    sumsq_bwd[label] += bwd_pkts^2

    sum_bps[label] += flow_bytes_s
    sumsq_bps[label] += flow_bytes_s^2

    sum_pps[label] += flow_pkts_s
    sumsq_pps[label] += flow_pkts_s^2

    # track min/max
    if (!(label in min_dur) || flow_duration < min_dur[label]) min_dur[label] = flow_duration
    if (!(label in max_dur) || flow_duration > max_dur[label]) max_dur[label] = flow_duration
    if (!(label in min_bps) || flow_bytes_s < min_bps[label]) min_bps[label] = flow_bytes_s
    if (!(label in max_bps) || flow_bytes_s > max_bps[label]) max_bps[label] = flow_bytes_s
}

END {
    out = "out/signals.tsv"
    print "Label\tCount\tFlowDuration_Mean\tFlowDuration_Std\tFlowDuration_Min\tFlowDuration_Max\tFlowBytesPerSec_Mean\tFlowBytesPerSec_Std\tFlowBytesPerSec_Min\tFlowBytesPerSec_Max" > out
    for (lbl in count) {
        n = count[lbl]
        mean_dur = sum_dur[lbl]/n
        var_dur = (sumsq_dur[lbl]/n) - (mean_dur^2)
        std_dur = (var_dur>0 ? sqrt(var_dur) : 0)

        mean_bps = sum_bps[lbl]/n
        var_bps = (sumsq_bps[lbl]/n) - (mean_bps^2)
        std_bps = (var_bps>0 ? sqrt(var_bps) : 0)

        printf "%s\t%d\t%.3f\t%.3f\t%.1f\t%.1f\t%.3f\t%.3f\t%.1f\t%.1f\n", \
            lbl, n, mean_dur, std_dur, min_dur[lbl], max_dur[lbl], mean_bps, std_bps, min_bps[lbl], max_bps[lbl] >> out
    }
}' out/filtered.tsv

awk -F'\t' -v OFS='\t' '
NR==1 { print $0 "\tOutlierFlag" > "out/signals_outliers.tsv"; next }
{
    mean_bps=$7; std_bps=$8; max_bps=$10;
    outlier = (std_bps>0 && (max_bps-mean_bps)/std_bps>3 ? "YES" : "NO");
    print $0, outlier >> "out/signals_outliers.tsv";
}' out/signals.tsv

# rank labels by mean FlowBytes descending
(head -n1 out/signals.tsv && tail -n +2 out/signals.tsv | sort -t$'\t' -k7,7nr -k1,1) > out/signals_ranked.tsv

echo "Signal discovery complete → out/signals.tsv, out/signals_outliers.tsv, out/signals_ranked.tsv" | tee -a "$LOG"
