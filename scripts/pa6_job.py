from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, desc
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time, datetime

# Spark session
spark = SparkSession.builder.appName("CybersecurityAnalysis").getOrCreate()

# Paths, change paths to your bucket when using. Also download dataset before running
DATASET = "gs://pa6-cs131-vt/Wednesday-workingHours.pcap_ISCX.csv"
OUT_DIR = "gs://pa6-cs131-vt/output"

print(f"{datetime.datetime.now()} - Starting analysis on {DATASET}")

# Load CSV
df = (spark.read.option("header", True)
      .option("inferSchema", True)
      .option("delimiter", ",")
      .csv(DATASET))
df = df.toDF(*[c.strip() for c in df.columns]) # Removing white spacing before and after 
print(f"{datetime.datetime.now()} - Loaded {df.count()} rows, {len(df.columns)} columns.") # Make sure the loaded # are the same as ur expected in the downloaded DS

# Filter and select important columns
quality = (df.select(
                col("Label"),
                col("Destination Port").cast("long").alias("Destination_Port"),
                col("Flow Duration").cast("long").alias("Flow_Duration")
            )
            .filter(
                col("Label").isNotNull() &
                col("Destination_Port").isNotNull() &
                col("Destination_Port").between(0, 65535) &
                col("Flow_Duration").isNotNull()
            ))

# Frequency table by Label
freq_label = (quality.groupBy("Label")
              .agg(count(F.lit(1)).alias("Count"))
              .orderBy(desc("Count"), "Label"))

# Top 20 Destination Ports
port_counts = quality.groupBy("Destination_Port").agg(count(F.lit(1)).alias("Count"))
ranked = port_counts.withColumn("rank", F.dense_rank().over(Window.orderBy(desc("Count"), "Destination_Port")))
top_ports = ranked.filter(col("rank") <= 20).orderBy("rank")

# Add is_malicious column
quality = quality.withColumn("is_malicious", when(col("Label") != "BENIGN", 1).otherwise(0))

# Correlation
correlation = quality.stat.corr("Flow_Duration", "is_malicious")
print(f"{datetime.datetime.now()} - Flow Duration vs is_malicious correlation: {correlation}")

# Flow summary per category
flow_summary = (quality.groupBy("is_malicious")
                .agg(F.mean("Flow_Duration").alias("Mean_Flow_Duration"),
                     F.stddev("Flow_Duration").alias("StdDev_Flow_Duration"),
                     F.max("Flow_Duration").alias("Max_Flow_Duration"),
                     F.min("Flow_Duration").alias("Min_Flow_Duration")))

# Write outputs
freq_label.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT_DIR}/freq_label")
top_ports.select("Destination_Port", "Count", "rank").coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT_DIR}/top_ports")
flow_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT_DIR}/flow_summary")

# Show outputs
freq_label.show(20, False)
top_ports.show(20, False)
flow_summary.show(truncate=False)

# Timer to keep Spark UI alive for 3 minutes
print(f"{datetime.datetime.now()} - Keeping Spark UI alive for 3 minutes...")
time.sleep(180)
print(f"{datetime.datetime.now()} - Timer done. Stopping Spark.")
spark.stop()
