import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { STREAMING_TRANSACTIONS_CSV } from './_canonical'

const EVENT_TIME_WATERMARK_PY = `"""
Lab 37: Event-Time Processing & Watermarking
Tumble and slide window metrics while bounding state store memory with Watermarks.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, window, sum, count

def main():
    spark = SparkSession.builder.appName("WatermarkingApp").master("local[*]").getOrCreate()

    tx_schema = StructType([
        StructField("tx_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("card_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("merchant", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    streaming_df = (
        spark.readStream
        .schema(tx_schema)
        .option("header", "true")
        .csv("stream_input/")
    )

    # 1. Define Watermark: Accept events up to 10 minutes late relative to max event-time seen
    # 2. Window Aggregation: 5-minute tumbling windows on event-time
    windowed_aggregates = (
        streaming_df
        .withWatermark("timestamp", "10 minutes")
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("merchant")
        )
        .agg(
            sum("amount").alias("window_volume"),
            count("tx_id").alias("tx_count")
        )
    )

    print("=== STREAMING WATERMARKED AGGREGATION PLAN ===")
    windowed_aggregates.explain()

    query = (
        windowed_aggregates.writeStream
        .format("memory")
        .queryName("merchant_velocity")
        .outputMode("complete")
        .start()
    )

    query.processAllAvailable()
    query.stop()

    print("✓ Watermarked event-time window aggregation completed.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson37: Lesson = {
  id: 137,
  title: 'Spark Lab 37 · Event-Time Processing & Watermarking',
  concept: `In streaming systems, network delays and device disconnections mean events frequently arrive out of order:
- **Event Time**: The moment the transaction or click occurred on the client device (e.g. \`2024-04-01 12:00:10\`).
- **Processing Time**: The moment Spark's executor processes the packet (e.g. 5 minutes later).

If you aggregate metrics by Event Time (e.g. "Total revenue per 5-minute window"), Spark must keep partial sums in an in-memory **State Store** waiting for potential late arrivals.

### The Problem: Infinite State Store Growth
If a mobile phone goes offline and uploads its transactions 3 weeks later, keeping all past windows in memory will quickly exhaust the cluster's RAM and crash Spark with OutOfMemory!

### The Solution: Watermarking
A **Watermark** tells Spark how late data can arrive before being discarded forever:
\`\`\`python
streaming_df.withWatermark("timestamp", "10 minutes")
\`\`\`
- Spark tracks the maximum event time seen so far: \`max_event_time\`.
- The current watermark threshold is: \`watermark = max_event_time - 10 minutes\`.
- Any window whose end-time is older than the watermark is **evicted from the State Store**, reclaiming RAM!
- Any new records arriving with \`timestamp < watermark\` are dropped as too late.`,
  initialFiles: {
    'event_time_watermark.py': EVENT_TIME_WATERMARK_PY,
    'stream_input/transactions.csv': STREAMING_TRANSACTIONS_CSV,
  },
  tasks: [
    {
      id: 'verify_watermark_window',
      prompt: "Ensure `event_time_watermark.py` includes `withWatermark('timestamp', '10 minutes')` and `window(...)` aggregation.",
      hint: "Check withWatermark and window function calls in the script.",
      validate: (s) =>
        fileContains(s, 'event_time_watermark.py', 'withWatermark') &&
        fileContains(s, 'event_time_watermark.py', 'window('),
    },
    {
      id: 'run_watermark_script',
      prompt: 'Run the watermarked streaming pipeline: `spark-submit event_time_watermark.py`.',
      hint: 'Type `spark-submit event_time_watermark.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('event_time_watermark') ||
        Boolean(s.lastRun?.args?.includes('event_time_watermark.py')),
    },
  ],
  quiz: {
    question: 'What is the primary operational purpose of withWatermark() in Spark Structured Streaming?',
    options: [
      'To add a copyright image to exported reports',
      'To bound State Store memory by allowing Spark to drop late data and evict old aggregate windows',
      'To slow down the cluster when CPU temperatures rise',
      'To encrypt communication between workers',
    ],
    correctIndex: 1,
    explanation: 'Watermarking establishes a cutoff threshold (max event time minus delay), allowing Spark to safely drop ancient records and clear completed window states from memory.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw', label: 'Streaming Events (Event-Time)', layer: 'source' },
        { id: 'watermark', label: 'withWatermark (10 min threshold)', layer: 'staging' },
        { id: 'state', label: 'Bounded State Store (5-min Windows)', layer: 'mart' },
      ],
      edges: [
        { source: 'raw', target: 'watermark' },
        { source: 'watermark', target: 'state' },
      ],
    },
  },
  furtherReading: [
    { label: 'Handling Late Data and Watermarking in Spark', url: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking' },
  ],
}

export default sparkLesson37
