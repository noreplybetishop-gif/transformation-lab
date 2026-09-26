import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { USERS_CSV } from './_canonical'

const DELTA_TRANSACTIONS_PY = `"""
Lab 22: Delta Lake ACID Transactions
Implement ACID transactions, schema enforcement, and atomic commits on a modern Lakehouse.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def main():
    # 1. Initialize SparkSession with Delta Lake package configs
    spark = (
        SparkSession.builder
        .appName("DeltaLakeLab")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    users_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv")

    # 2. Write initial table as Delta Lake format
    (
        users_df
        .write
        .format("delta")
        .mode("overwrite")
        .save("output/delta_users")
    )
    print("=== Successfully Created Delta Lake Table (Version 0 Committed) ===")

    # 3. Perform an atomic append transaction
    new_users = spark.createDataFrame([
        (9, "Ian Wright", 33, "Manchester", "UK", "2024-03-01"),
        (10, "Julia Roberts", 29, "Los Angeles", "US", "2024-03-05")
    ], schema=users_df.schema)

    (
        new_users
        .write
        .format("delta")
        .mode("append")
        .save("output/delta_users")
    )
    print("=== Appended 2 New Records via ACID Commit (Version 1 Committed) ===")

    # 4. Read latest Delta snapshot
    current_delta_df = spark.read.format("delta").load("output/delta_users")
    print(f"Total Rows in Delta Table: {current_delta_df.count()}")
    current_delta_df.orderBy(col("id").desc()).show(5)

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson22: Lesson = {
  id: 122,
  title: 'Spark Lab 22 · Delta Lake ACID Transactions',
  concept: `Traditional data lakes built on raw Parquet or CSV suffer from major reliability flaws:
- **No ACID Transactions**: Concurrent writes cause corrupt partial reads. If a write fails halfway, garbage files are left behind.
- **No Schema Enforcement**: Bad data can quietly introduce invalid types.
- **No Upserts**: Updating a few rows requires rewriting the entire table partition.

### Delta Lake: The Foundation of the Lakehouse
**Delta Lake** is an open-source storage layer that brings ACID reliability to Apache Spark:
1. **The Transaction Log (\`_delta_log/\`)**: Every commit writes an atomic JSON commit record (\`000000.json\`). A transaction either completes 100% or is rolled back.
2. **Schema Enforcement & Evolution**: Rejects writes with missing or altered columns unless explicitly authorized (\`.option("mergeSchema", "true")\`).
3. **Unified Batch & Streaming**: Supports streaming ingestion and batch queries simultaneously on the same table.

\`\`\`python
# Writing to Delta Lake:
df.write.format("delta").mode("append").save("/lakehouse/silver_orders")
\`\`\`

In this lab, you will configure Delta Lake catalog extensions, perform atomic write commits, and inspect the updated table state.`,
  initialFiles: {
    'delta_transactions.py': DELTA_TRANSACTIONS_PY,
    'data/users.csv': USERS_CSV,
  },
  tasks: [
    {
      id: 'verify_delta_code',
      prompt: "Examine `delta_transactions.py` to confirm the use of `.format('delta')` and Delta catalog configurations.",
      hint: "Check that `.format('delta')` is used in both the initial write and the append transaction.",
      validate: (s) =>
        fileContains(s, 'delta_transactions.py', 'format("delta")') ||
        fileContains(s, 'delta_transactions.py', "format('delta')"),
    },
    {
      id: 'run_delta_job',
      prompt: 'Execute the Delta Lake transactions pipeline: `spark-submit delta_transactions.py`.',
      hint: 'Type `spark-submit delta_transactions.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('delta_transactions') ||
        Boolean(s.lastRun?.args?.includes('delta_transactions.py')),
    },
  ],
  quiz: {
    question: 'How does Delta Lake guarantee ACID transactions and prevent dirty reads during ongoing writes?',
    options: [
      'It locks the entire hard drive until all jobs finish',
      'It uses an ordered JSON transaction log (_delta_log) that commits atomically only when all data files are completely written',
      'It requires single-node execution',
      'It deletes older versions of the file immediately',
    ],
    correctIndex: 1,
    explanation: 'Delta Lake uses an append-only transaction log (_delta_log) with mutual exclusion. Readers only see files registered in an already-committed transaction log entry.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'csv_data', label: 'users.csv', layer: 'source' },
        { id: 'delta_commit_v0', label: 'Delta Commit V0', layer: 'staging' },
        { id: 'delta_commit_v1', label: 'ACID Append V1', layer: 'mart' },
      ],
      edges: [
        { source: 'csv_data', target: 'delta_commit_v0' },
        { source: 'delta_commit_v0', target: 'delta_commit_v1' },
      ],
    },
  },
  furtherReading: [
    { label: 'Delta Lake Official Documentation', url: 'https://docs.delta.io/latest/index.html' },
    { label: 'Delta Lake ACID Guarantees', url: 'https://docs.delta.io/latest/concurrency-control.html' },
  ],
}

export default sparkLesson22
