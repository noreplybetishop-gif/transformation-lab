import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { STREAMING_TRANSACTIONS_CSV } from './_canonical'

const DELTA_STREAMING_CDC_PY = `"""
Lab 40: Delta Lake Streaming & Change Data Capture (CDC)
Stream events into Delta Lake and consume row-level Change Data Feed (CDF).
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder
        .appName("DeltaStreamingCDC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
        .master("local[*]")
        .getOrCreate()
    )

    tx_schema = StructType([
        StructField("tx_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("card_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("merchant", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    print("=== 1. STREAMING INGESTION TO DELTA LAKE SINK ===")
    stream_df = spark.readStream.schema(tx_schema).option("header", "true").csv("stream_input/")

    # Write stream into an ACID Delta table with checkpointing
    delta_writer = (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "checkpoints/delta_tx_ingest")
        .start("lakehouse/bronze/transactions_delta")
    )

    delta_writer.processAllAvailable()
    delta_writer.stop()

    print("✓ Transactions ingested to Delta Lake Bronze table.")

    print("=== 2. READING CHANGE DATA FEED (CDF) FROM DELTA ===")
    # Change Data Feed reads row-level insert, update_preimage, update_postimage, and delete events
    cdf_df = (
        spark.read
        .format("delta")
        .option("readChangeData", "true")
        .option("startingVersion", 0)
        .load("lakehouse/bronze/transactions_delta")
    )

    print("Delta Lake CDF Schema & Mutation Metadata:")
    cdf_df.printSchema()

    print("✓ Delta Lake Streaming and CDF validated.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson40: Lesson = {
  id: 140,
  title: 'Spark Lab 40 · Delta Lake Streaming & Change Data Capture (CDC)',
  concept: `Traditional cloud data lakes (raw S3/GCS parquet files) make concurrent reading and writing during streaming impossible without causing corrupted partial reads or file collision errors.

**Delta Lake** solves this natively:
- Delta Lake acts as both a **Streaming Sink** and a **Streaming Source**!
- Ingestion writes are recorded into the \`_delta_log\` as atomic transactions with ACID serializable isolation.

### Change Data Feed (CDF)
Modern lakehouse architectures frequently need to capture mutations (inserts, updates, deletes) from upstream transactional databases (MySQL, Postgres, DynamoDB via Debezium or Fivetran).

When you enable Change Data Feed:
\`\`\`python
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
\`\`\`
Delta automatically generates internal metadata columns:
- \`_change_type\`: \`insert\`, \`update_preimage\`, \`update_postimage\`, or \`delete\`.
- \`_commit_version\`: The Delta transaction commit number.
- \`_commit_timestamp\`: Exact millisecond timestamp of the commit.

Downstream consumers (e.g. Silver or Gold aggregation tables) can read **only the changes** using:
\`\`\`python
spark.readStream.format("delta").option("readChangeFeed", "true").load(delta_path)
\`\`\`
This enables true incremental micro-batch lakehouse pipelines without expensive full table rescans!`,
  initialFiles: {
    'delta_streaming_cdc.py': DELTA_STREAMING_CDC_PY,
    'stream_input/transactions.csv': STREAMING_TRANSACTIONS_CSV,
  },
  tasks: [
    {
      id: 'verify_delta_streaming_cdc',
      prompt: "Ensure `delta_streaming_cdc.py` writes to format('delta') and loads with option('readChangeData', 'true').",
      hint: "Check format('delta') and option('readChangeData', 'true') are in the script.",
      validate: (s) =>
        fileContains(s, 'delta_streaming_cdc.py', "format('delta')") ||
        fileContains(s, 'delta_streaming_cdc.py', 'format("delta")') &&
        fileContains(s, 'delta_streaming_cdc.py', 'readChangeData'),
    },
    {
      id: 'run_delta_cdc_script',
      prompt: 'Run the Delta streaming CDC pipeline: `spark-submit delta_streaming_cdc.py`.',
      hint: 'Type `spark-submit delta_streaming_cdc.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('delta_streaming_cdc') ||
        Boolean(s.lastRun?.args?.includes('delta_streaming_cdc.py')),
    },
  ],
  quiz: {
    question: 'What metadata column is added by Delta Lake Change Data Feed (CDF) to identify mutations?',
    options: [
      '_change_type (insert, update_preimage, update_postimage, delete)',
      '_secret_password',
      '_cpu_temperature',
      '_network_ping',
    ],
    correctIndex: 0,
    explanation: 'Delta Lake Change Data Feed introduces `_change_type` alongside commit metadata, allowing downstream pipelines to propagate updates and deletes without table scans.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'source', label: 'Streaming Tx Stream', layer: 'source' },
        { id: 'delta_bronze', label: 'Delta Bronze Table (ACID Log)', layer: 'staging' },
        { id: 'cdf', label: 'Change Data Feed (Row Mutations)', layer: 'mart' },
      ],
      edges: [
        { source: 'source', target: 'delta_bronze' },
        { source: 'delta_bronze', target: 'cdf' },
      ],
    },
  },
  furtherReading: [
    { label: 'Delta Lake Change Data Feed Documentation', url: 'https://docs.delta.io/latest/delta-change-data-feed.html' },
  ],
}

export default sparkLesson40
