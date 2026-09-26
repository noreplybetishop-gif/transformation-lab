import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { STREAMING_TRANSACTIONS_CSV } from './_canonical'

const STREAMING_BASICS_PY = `"""
Lab 36: Structured Streaming Fundamentals
Process continuous micro-batches using Spark Structured Streaming.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("StructuredStreamingBasics").master("local[*]").getOrCreate()

    # Define streaming schema explicitly (streaming requires rigid schemas)
    tx_schema = StructType([
        StructField("tx_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("card_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("merchant", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    print("=== INITIALIZING STRUCTURED STREAMING SOURCE ===")
    # readStream creates an unbounded streaming DataFrame
    streaming_df = (
        spark.readStream
        .schema(tx_schema)
        .option("header", "true")
        .option("maxFilesPerTrigger", 1)
        .csv("stream_input/")
    )

    # Filter high-value transactions
    alerts_stream = streaming_df.filter(col("amount") > 500.0)

    print(f"Is DataFrame streaming? {streaming_df.isStreaming}")

    # Output sink configuration (Memory or Console sink)
    query = (
        alerts_stream.writeStream
        .format("memory")
        .queryName("high_value_alerts")
        .outputMode("append")
        .start()
    )

    print(f"Active Query ID: {query.id}")
    print(f"Query Status: {query.status['message']}")

    # Process batch 0 and stop
    query.processAllAvailable()
    query.stop()

    print("✓ Structured Streaming pipeline executed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson36: Lesson = {
  id: 136,
  title: 'Spark Lab 36 · Structured Streaming Fundamentals',
  concept: `**Spark Structured Streaming** is a scalable and fault-tolerant stream processing engine built on top of the Spark SQL Catalyst engine.

### The Unbounded Table Model
Instead of thinking about streams as individual message queues, Structured Streaming treats incoming data streams as an **unbounded table**:
- New data arriving in the stream is appended as new rows to the unbounded table.
- The developer writes standard DataFrame queries (\`filter\`, \`select\`, \`join\`, \`groupBy\`).
- Spark automatically runs incremental micro-batch execution (or continuous processing mode) under the hood!

\`\`\`
Incoming Stream ──► [ Unbounded Table (readStream) ] ──► [ Catalyst Query ] ──► [ Output Sink (writeStream) ]
\`\`\`

### Critical Streaming Concepts
1. **Schema Enforcement**: Unlike batch reading, \`readStream\` **requires an explicit schema** via \`.schema(schema)\` because Spark cannot scan future incoming files to infer schemas.
2. **Output Modes**:
   - **Append**: Only new rows added to the result table are emitted to the sink.
   - **Complete**: The entire updated result table is re-written to the sink on every trigger (used with aggregations).
   - **Update**: Only the rows that were updated since the last trigger are emitted.
3. **Triggers**: Control timing, e.g. \`.trigger(processingTime="10 seconds")\` or \`.trigger(availableNow=True)\`.`,
  initialFiles: {
    'streaming_basics.py': STREAMING_BASICS_PY,
    'stream_input/transactions.csv': STREAMING_TRANSACTIONS_CSV,
  },
  tasks: [
    {
      id: 'verify_streaming_read',
      prompt: "Ensure `streaming_basics.py` configures `spark.readStream` with `tx_schema` and `writeStream`.",
      hint: "Check that spark.readStream.schema(tx_schema) and writeStream are present in the script.",
      validate: (s) =>
        fileContains(s, 'streaming_basics.py', 'readStream') &&
        fileContains(s, 'streaming_basics.py', 'writeStream') &&
        fileContains(s, 'streaming_basics.py', 'outputMode'),
    },
    {
      id: 'run_streaming_script',
      prompt: 'Execute the streaming job: `spark-submit streaming_basics.py`.',
      hint: 'Type `spark-submit streaming_basics.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('streaming_basics') ||
        Boolean(s.lastRun?.args?.includes('streaming_basics.py')),
    },
  ],
  quiz: {
    question: 'Why does Spark Structured Streaming require an explicit schema on `readStream`?',
    options: [
      'It cannot infer schema from future incoming streaming files that have not arrived yet',
      'Because streaming only supports plain text files without columns',
      'The Python interpreter crashes without StructType',
      'It is an arbitrary rule with no technical purpose',
    ],
    correctIndex: 0,
    explanation: 'Streaming sources are unbounded and continuous. Spark cannot perform a preliminary scan of the entire dataset to infer types, so an explicit schema must be declared.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'stream', label: 'readStream (Unbounded CSV)', layer: 'source' },
        { id: 'filter', label: 'Streaming Filter (amount > 500)', layer: 'staging' },
        { id: 'sink', label: 'writeStream (Memory Sink)', layer: 'mart' },
      ],
      edges: [
        { source: 'stream', target: 'filter' },
        { source: 'filter', target: 'sink' },
      ],
    },
  },
  furtherReading: [
    { label: 'Structured Streaming Programming Guide', url: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html' },
  ],
}

export default sparkLesson36
