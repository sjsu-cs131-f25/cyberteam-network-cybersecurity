#!/usr/bin/env bash
# ===================================================
# PA3: Finding structure in large datasets
# ===================================================
set -euo pipefail

# --- CONFIG VARIABLES ---
INPUT_CSV="/mnt/scratch/CS131_jelenag/projects/team09_sec3/data/samples/sample1k.csv"
INPUT_TSV="/mnt/scratch/CS131_jelenag/projects/team09_sec3/data/samples/sample1k.tsv"

EDGE_LEFT_COL=79   
EDGE_RIGHT_COL=2   

THRESHOLD=3         

OUTDIR="/mnt/scratch/CS131_jelenag/projects/team09_sec3/out"
mkdir -p "$OUTDIR"
# ===================================================
# STEP 1: Build edges.tsv, Extract LeftEntity and RightEntity columns from CSV, convert CSV commas to tabs, sort by LeftEntity
# ===================================================
echo "[Step 1] Building edges.tsv..."
sed -E 's/("([^"]*)")?,/\2\t/g' "$INPUT_CSV" > "$OUTDIR/temp.tsv"
cut -f${EDGE_LEFT_COL},${EDGE_RIGHT_COL} "$OUTDIR/temp.tsv" | sort -k1,1 > "$OUTDIR/edges.tsv"
rm "$OUTDIR/temp.tsv"
echo "edges.tsv created at $OUTDIR/edges.tsv"

# ===================================================
# STEP 2: Filter significant clusters Count occurrences of each left entity, keep only entities meeting the threshold, and extract their edges
# ===================================================
echo "[Step 2] Filtering significant clusters..."
cut -f1 "$OUTDIR/edges.tsv" | sort | uniq -c | sort -nr > "$OUTDIR/entity_counts.tsv"
awk -v n="$THRESHOLD" '$1 >= n {print $2}' "$OUTDIR/entity_counts.tsv" > "$OUTDIR/significant_entities.txt"
grep -Ff "$OUTDIR/significant_entities.txt" "$OUTDIR/edges.tsv" > "$OUTDIR/edges_thresholded.tsv"
echo "edges_thresholded.tsv created"

# ===================================================
# STEP 3: Compute cluster sizes for histogram, Count number of edges per left entity and save
# ===================================================
echo "[Step 3] Computing cluster sizes..."
cut -f1 "$OUTDIR/edges_thresholded.tsv" | sort | uniq -c | awk '{print $2 "\t" $1}' > "$OUTDIR/cluster_sizes.tsv"
echo "cluster_sizes.tsv created"
echo "You can plot this using gnuplot, Excel, or Google Sheets"

# ===================================================
# STEP 4: Compute Top-30 tokens, Find the most frequent RightEntity tokens in thresholded clusters and overall dataset
# ===================================================
echo "[Step 4] Computing Top-30 tokens..."
cut -f2 "$OUTDIR/edges_thresholded.tsv" | sort | uniq -c | sort -nr | head -30 > "$OUTDIR/top30_clusters.txt"
cut -f2 "$OUTDIR/edges.tsv" | sort | uniq -c | sort -nr | head -30 > "$OUTDIR/top30_overall.txt"

# Compare Top-30 cluster tokens vs overall tokens
comm -23 <(cut -d' ' -f2 "$OUTDIR/top30_clusters.txt" | sort) \
         <(cut -d' ' -f2 "$OUTDIR/top30_overall.txt" | sort) \
         > "$OUTDIR/diff_top30.txt"
echo "Top-30 token comparison done"

echo "Steps 1–4 complete. All outputs are in '$OUTDIR/'"

# ===================================================
# Step 5: Use Cytoscape to create cluster_viz.png
# ===================================================

# ===================================================
# Step 6: Compute summary statistics for "Total Length of Fwd Packets" grouped by "Flow Duration"
# ===================================================

# Check if files exist
if [ ! -f "$OUTDIR/edges_thresholded.tsv" ]; then
    echo "Error: edges_thresholded.tsv not found."
    exit 1
fi

if [ ! -f  "$INPUT_TSV" ]; then
    echo "Error: sample1k.tsv not found."
    exit 1
fi

# Sort edges_thresholded.tsv by label
sort -k2,2 -t $'\t' "$OUTDIR/edges_thresholded.tsv" > "$OUTDIR/edges_thresholded_sorted.tsv"

# Sort dataset by label (79th column)
sort -k79,79 -t $'\t' "$INPUT_TSV" > "$OUTDIR/dataset_sorted.tsv"

# Join files: Output first column from edges (Flow Duration) and 5th column from original (Total Length of Fwd Packets)
join -1 2 -2 79 -t $'\t' -o '1.1,2.5' "$OUTDIR/edges_thresholded_sorted.tsv" "$OUTDIR/dataset_sorted.tsv" > "$OUTDIR/joined_file.tsv"

# Sort left_outcome.tsv by the first column
sort -k1,1 -n -t $'\t' "$OUTDIR/joined_file.tsv" > "$OUTDIR/left_outcome_sorted.tsv"

# Run datamash: Group by first column and compute count, mean, median of second column
datamash -g 1 count 2 mean 2 median 2 < "$OUTDIR/left_outcome_sorted.tsv" > "$OUTDIR/cluster_outcomes.tsv"

echo "Script complete! Output file: out/cluster_outcomes.tsv"

