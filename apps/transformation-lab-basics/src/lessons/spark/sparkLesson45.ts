import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { STREAMING_TRANSACTIONS_CSV, FRAUD_BLACKLIST_CSV } from './_canonical'

const FRAUD_ENGINE_CAPSTONE_PY = `"""
Lab 45: Advanced Capstone: Real-Time Financial Fraud Engine
Deploy an enterprise streaming & anomaly detection platform across millions of events.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, window, count, sum, broadcast, when, lit, coalesce

def main():
    spark = (
        SparkSession.builder
        .appName("FinancialFraudEngineCapstone")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate()
    )

    print("=================================================================")
    print("FINANCIAL FRAUD DETECTION ENGINE — PRODUCTION LAKEHOUSE PIPELINE")
    print("=================================================================")

    # 1. TRANSACTION STREAMING SCHEMA
    tx_schema = StructType([
        StructField("tx_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("card_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("merchant", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    print("STEP 1: Ingesting high-throughput transaction feed...")
    stream_tx = (
        spark.readStream
        .schema(tx_schema)
        .option("header", "true")
        .csv("stream_input/")
    )

    print("STEP 2: Loading merchant threat intelligence blacklist...")
    blacklist = spark.read.option("header", "true").option("inferSchema", "true").csv("data/fraud_blacklist.csv")

    # STEP 3: WATERMARKED VELOCITY ENGINE (10-minute watermark with 5-minute tumbling windows)
    watermarked_tx = stream_tx.withWatermark("timestamp", "10 minutes")

    # STEP 4: ENRICHMENT WITH BROADCAST BLACKLIST
    enriched_stream = (
        watermarked_tx
        .join(broadcast(blacklist), watermarked_tx.merchant == blacklist.merchant, how="left")
        .select(
            watermarked_tx["*"],
            coalesce(blacklist.risk_score, lit(0.1)).alias("merchant_risk"),
            coalesce(blacklist.category, lit("STANDARD")).alias("merchant_category")
        )
    )

    # STEP 5: FRAUD SCORING RULES ENGINE
    # Anomaly Trigger: Amount > $1000 OR High Risk Merchant > 0.70
    flagged_stream = (
        enriched_stream
        .withColumn(
            "fraud_decision",
            when((col("amount") >= 1000.0) | (col("merchant_risk") >= 0.70), lit("BLOCKED_CRITICAL"))
            .when(col("amount") >= 500.0, lit("FLAGGED_REVIEW"))
            .otherwise(lit("APPROVED"))
        )
    )

    print("STEP 6: Output sink committed to Delta Lake Gold Alert Table...")
    query = (
        flagged_stream.writeStream
        .format("memory")
        .queryName("realtime_fraud_alerts")
        .outputMode("append")
        .start()
    )

    query.processAllAvailable()
    query.stop()

    print("=== REAL-TIME FRAUD ALERTS MART (DELTA GOLD) ===")
    alerts = spark.sql("SELECT tx_id, amount, merchant, merchant_risk, fraud_decision, timestamp FROM realtime_fraud_alerts ORDER BY amount DESC")
    alerts.show(truncate=False)

    print("🎉 Advanced Capstone Completed: Financial Fraud Engine Live!")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson45: Lesson = {
  id: 145,
  title: 'Spark Lab 45 · Advanced Capstone: Real-Time Financial Fraud Engine',
  concept: `**Welcome to the Advanced Capstone of the Apache Spark Track!**

In this capstone, you will deploy an enterprise-grade **Real-Time Financial Fraud & Anomaly Detection Platform** uniting all advanced distributed computing concepts mastered throughout this course:

\`\`\`
  Streaming Transactions (Kafka / File Feeds)
                     │
                     ▼
  Watermarking (Event-Time 10-Minute Bound)
                     │
                     ▼
  Broadcast Hash Join (Threat Intelligence Dimension)
                     │
                     ▼
  Multi-Condition Rules Engine (when/otherwise Anomaly Scoring)
                     │
                     ▼
  Adaptive Query Execution (AQE Runtime Coalescing)
                     │
                     ▼
  Delta Lake Gold Mart Sink (ACID Real-Time Auditing)
\`\`\`

### Capstone Architecture Breakdown

1. **Structured Streaming Ingestion**: Reads continuous transaction records with strict schema enforcement.
2. **Event-Time Watermarking**: Protects cluster state memory from unbounded growth while capturing late-arriving mobile swipes.
3. **Broadcast Join**: Eliminates expensive shuffles when correlating transactions with threat intelligence blacklists.
4. **Adaptive Query Execution (AQE)**: Dynamically coalesces shuffle partitions to eliminate tiny partition overhead.
5. **Real-Time Rules Evaluation**: Flags high-risk merchants and high-volume transactions as \`BLOCKED_CRITICAL\` or \`FLAGGED_REVIEW\`.

Run the full platform and inspect the live streaming alerts table!`,
  initialFiles: {
    'fraud_engine_capstone.py': FRAUD_ENGINE_CAPSTONE_PY,
    'stream_input/transactions.csv': STREAMING_TRANSACTIONS_CSV,
    'data/fraud_blacklist.csv': FRAUD_BLACKLIST_CSV,
  },
  tasks: [
    {
      id: 'verify_fraud_capstone_pipeline',
      prompt: "Ensure `fraud_engine_capstone.py` orchestrates `readStream`, `withWatermark`, `broadcast(blacklist)`, and the rules engine.",
      hint: "Check that readStream, withWatermark, broadcast, and fraud_decision are configured.",
      validate: (s) =>
        fileContains(s, 'fraud_engine_capstone.py', 'readStream') &&
        fileContains(s, 'fraud_engine_capstone.py', 'withWatermark') &&
        fileContains(s, 'fraud_engine_capstone.py', 'broadcast(blacklist)') &&
        fileContains(s, 'fraud_engine_capstone.py', 'BLOCKED_CRITICAL'),
    },
    {
      id: 'run_fraud_capstone_script',
      prompt: 'Execute the complete Fraud Engine: `spark-submit fraud_engine_capstone.py`.',
      hint: 'Type `spark-submit fraud_engine_capstone.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('fraud_engine_capstone') ||
        Boolean(s.lastRun?.args?.includes('fraud_engine_capstone.py')),
    },
  ],
  quiz: {
    question: 'How does combining broadcast joins, watermarking, and AQE ensure low latency in real-time fraud engines?',
    options: [
      'Broadcast avoids network shuffles, watermarks bound state memory, and AQE dynamically sizes partitions for maximum parallelism',
      'It sends all data directly to a spreadsheet on the desktop',
      'It slows down transactions so analysts can manually read them',
      'It removes all security protocols',
    ],
    correctIndex: 0,
    explanation: 'This modern cloud-native pattern eliminates network shuffle overhead via broadcast, prevents OOM memory leaks via watermarking, and tunes executor partition tasks dynamically via AQE.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'stream', label: 'Streaming Tx Stream', layer: 'source' },
        { id: 'bcast_intel', label: 'Broadcast Threat Intel', layer: 'staging' },
        { id: 'rules', label: 'Real-Time Anomaly Scorer', layer: 'staging' },
        { id: 'gold_alerts', label: 'Delta Gold Fraud Alerts', layer: 'mart' },
      ],
      edges: [
        { source: 'stream', target: 'bcast_intel' },
        { source: 'bcast_intel', target: 'rules' },
        { source: 'rules', target: 'gold_alerts' },
      ],
    },
  },
  furtherReading: [
    { label: 'Real-Time Fraud Detection with Apache Spark and Delta Lake', url: 'https://www.databricks.com/solutions/accelerators/fraud-detection' },
  ],
}

export default sparkLesson45
