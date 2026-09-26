import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { STREAMING_TRANSACTIONS_CSV, FRAUD_BLACKLIST_CSV } from './_canonical'

const STREAM_JOINS_PY = `"""
Lab 38: Stream-Static & Stream-Stream Joins
Enrich high-velocity streaming records against static reference lookups.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, broadcast

def main():
    spark = SparkSession.builder.appName("StreamJoinsApp").master("local[*]").getOrCreate()

    tx_schema = StructType([
        StructField("tx_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("card_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("merchant", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    # 1. STREAMING FACT: Live transaction stream
    streaming_tx = (
        spark.readStream
        .schema(tx_schema)
        .option("header", "true")
        .csv("stream_input/")
    )

    # 2. STATIC DIMENSION: High-risk merchant blacklist table
    static_blacklist = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/fraud_blacklist.csv")
    )

    # 3. STREAM-STATIC JOIN: Broadcast static dimension to avoid streaming shuffles
    enriched_stream = (
        streaming_tx
        .join(broadcast(static_blacklist), streaming_tx.merchant == static_blacklist.merchant, how="inner")
        .select(
            streaming_tx.tx_id,
            streaming_tx.amount,
            streaming_tx.merchant,
            static_blacklist.risk_score,
            static_blacklist.category.alias("threat_level")
        )
    )

    print("=== STREAM-STATIC JOIN CATALYST PLAN ===")
    enriched_stream.explain()

    query = (
        enriched_stream.writeStream
        .format("memory")
        .queryName("flagged_merchants")
        .outputMode("append")
        .start()
    )

    query.processAllAvailable()
    query.stop()

    print("✓ Stream-Static join executed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson38: Lesson = {
  id: 138,
  title: 'Spark Lab 38 · Stream-Static & Stream-Stream Joins',
  concept: `Joining streaming data is one of the most powerful paradigms in modern streaming architectures. Spark supports two varieties:

### 1. Stream-Static Joins
Enriches an incoming stream against a static table (e.g. joining real-time credit card transactions with a database of customer addresses or merchant fraud scores):
\`\`\`python
streaming_tx.join(broadcast(static_dimension), on="merchant_id")
\`\`\`
- Because the static table is loaded in driver/executor memory, using \`broadcast()\` completely avoids shuffles.
- **Stateless**: Spark does not need to store prior transactions in a state store!

### 2. Stream-Stream Joins
Correlates two continuous streams (e.g. matching an \`ad_impression\` stream with an \`ad_click\` stream):
- **Stateful**: Both streams must keep buffered records in memory until a match arrives.
- **Mandatory Requirements**:
  1. Define watermarks on **both streams** so Spark knows when to discard unmatched records.
  2. Define a **time-range join condition** (e.g. \`click_time >= impression_time AND click_time <= impression_time + interval 1 hour\`).
  Without the time constraint, the state store would grow infinitely!`,
  initialFiles: {
    'stream_joins.py': STREAM_JOINS_PY,
    'stream_input/transactions.csv': STREAMING_TRANSACTIONS_CSV,
    'data/fraud_blacklist.csv': FRAUD_BLACKLIST_CSV,
  },
  tasks: [
    {
      id: 'verify_stream_join',
      prompt: "Ensure `stream_joins.py` performs a join between `streaming_tx` and `broadcast(static_blacklist)`.",
      hint: "Check that broadcast(static_blacklist) is joined with streaming_tx.",
      validate: (s) =>
        fileContains(s, 'stream_joins.py', 'broadcast(static_blacklist)') &&
        fileContains(s, 'stream_joins.py', 'readStream'),
    },
    {
      id: 'run_stream_join_script',
      prompt: 'Run the stream-static join script: `spark-submit stream_joins.py`.',
      hint: 'Type `spark-submit stream_joins.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('stream_joins') ||
        Boolean(s.lastRun?.args?.includes('stream_joins.py')),
    },
  ],
  quiz: {
    question: 'Why does a Stream-Stream join strictly require both watermarks and a time-range interval constraint?',
    options: [
      'To prevent the in-memory state store from retaining unmatched events indefinitely',
      'Because Spark SQL does not know how to join two tables otherwise',
      'To enforce that all events come from the same calendar year',
      'To convert timestamps into string formats',
    ],
    correctIndex: 0,
    explanation: 'Without a time-range condition and watermarks, Spark would have to buffer all historical events from both streams in RAM forever, because a match could theoretically arrive years later.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'stream', label: 'Streaming Tx Feed', layer: 'source' },
        { id: 'static', label: 'Static Fraud Blacklist', layer: 'source' },
        { id: 'bcast_join', label: 'Broadcast Stream-Static Join', layer: 'staging' },
        { id: 'alerts', label: 'Flagged High-Risk Sinks', layer: 'mart' },
      ],
      edges: [
        { source: 'stream', target: 'bcast_join' },
        { source: 'static', target: 'bcast_join' },
        { source: 'bcast_join', target: 'alerts' },
      ],
    },
  },
  furtherReading: [
    { label: 'Stream-Stream Joins in Apache Spark', url: 'https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#stream-stream-joins' },
  ],
}

export default sparkLesson38
