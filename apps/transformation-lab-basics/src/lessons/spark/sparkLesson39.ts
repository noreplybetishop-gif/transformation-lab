import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { STREAMING_TRANSACTIONS_CSV } from './_canonical'

const DEDUP_STREAMING_PY = `"""
Lab 39: Deduplication & Idempotent Stream Sinks
Eliminate duplicate events with watermarked dropDuplicates and checkpointing.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("DedupStreamingApp").master("local[*]").getOrCreate()

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

    # DEDUPLICATION WITH WATERMARKING:
    # 1. Define watermark on event-time column
    # 2. Call dropDuplicates with the primary identifier AND event timestamp
    deduped_stream = (
        streaming_df
        .withWatermark("timestamp", "30 minutes")
        .dropDuplicates(["tx_id", "timestamp"])
    )

    print("=== DEDUPLICATION STREAMING EXECUTION ===")
    deduped_stream.explain()

    query = (
        deduped_stream.writeStream
        .format("memory")
        .queryName("deduped_transactions")
        .option("checkpointLocation", "checkpoints/dedup_tx")
        .outputMode("append")
        .start()
    )

    query.processAllAvailable()
    query.stop()

    print("✓ Exactly-once deduplicated streaming completed.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson39: Lesson = {
  id: 139,
  title: 'Spark Lab 39 · Deduplication & Idempotent Stream Sinks',
  concept: `In distributed messaging systems (Kafka, AWS Kinesis, RabbitMQ), network retries and producer failures inherently result in **at-least-once delivery guarantees**: the consumer will inevitably receive duplicated records.

### Exactly-Once Processing via dropDuplicates()
Spark Structured Streaming provides native stateful deduplication using:
\`\`\`python
deduped = stream.withWatermark("timestamp", "30 minutes") \\
                .dropDuplicates(["event_id", "timestamp"])
\`\`\`

### Why Both event_id AND timestamp?
If you deduplicate on \`["event_id"]\` alone without a watermark, Spark must remember every single UUID seen from the beginning of time! The state store would grow forever.

By combining \`withWatermark\` and including the timestamp column in \`dropDuplicates\`:
1. Spark keeps seen \`event_id\` keys only within the watermark window (e.g. last 30 minutes).
2. When the watermark moves forward, old IDs are automatically evicted from memory.

### Checkpointing for Fault-Tolerance
Streaming pipelines must set \`.option("checkpointLocation", "/path/to/checkpoints")\`.
The checkpoint directory saves:
- **Write-Ahead Log (WAL)**: Exact offsets read from the source.
- **State Store Snapshots**: State files (RocksDB or HDFS-backed) to resume deduplication and aggregations after crashes without duplicate reprocessing.`,
  initialFiles: {
    'dedup_streaming.py': DEDUP_STREAMING_PY,
    'stream_input/transactions.csv': STREAMING_TRANSACTIONS_CSV,
  },
  tasks: [
    {
      id: 'verify_dedup_watermark',
      prompt: "Ensure `dedup_streaming.py` combines `withWatermark` and `dropDuplicates(['tx_id', 'timestamp'])`.",
      hint: "Check withWatermark and dropDuplicates calls in the script.",
      validate: (s) =>
        fileContains(s, 'dedup_streaming.py', 'withWatermark') &&
        fileContains(s, 'dedup_streaming.py', 'dropDuplicates'),
    },
    {
      id: 'run_dedup_script',
      prompt: 'Execute the deduplication pipeline: `spark-submit dedup_streaming.py`.',
      hint: 'Type `spark-submit dedup_streaming.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('dedup_streaming') ||
        Boolean(s.lastRun?.args?.includes('dedup_streaming.py')),
    },
  ],
  quiz: {
    question: 'Why should the timestamp column be included alongside the unique ID in streaming dropDuplicates()?',
    options: [
      'To allow Spark to evict old record IDs from memory once the watermark passes',
      'Because Spark SQL cannot hash strings without a date',
      'To format the output as a calendar',
      'It speeds up Python compilation time',
    ],
    correctIndex: 0,
    explanation: 'Including the timestamp column allows Spark to bind the deduplication state store to the watermark, clearing aged IDs from RAM and preventing memory leaks.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'kafka', label: 'At-Least-Once Input Stream', layer: 'source' },
        { id: 'dedup', label: 'dropDuplicates (Watermarked State)', layer: 'staging' },
        { id: 'sink', label: 'Exactly-Once Idempotent Sink', layer: 'mart' },
      ],
      edges: [
        { source: 'kafka', target: 'dedup' },
        { source: 'dedup', target: 'sink' },
      ],
    },
  },
  furtherReading: [
    { label: 'Streaming Deduplication in Spark Structured Streaming', url: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#streaming-deduplication' },
  ],
}

export default sparkLesson39
