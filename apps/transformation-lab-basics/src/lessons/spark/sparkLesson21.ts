import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV } from './_canonical'

const PARQUET_IO_PY = `"""
Lab 21: Writing Efficient Parquet Files
Harness columnar storage, Snappy compression, and metadata dictionary statistics.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("ParquetLab").getOrCreate()

    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")

    # 1. Write as Parquet with Snappy compression (the industry standard)
    (
        orders_df
        .coalesce(1)  # Consolidate into 1 optimal file to prevent small-files fragmentation
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet("output/orders_snappy.parquet")
    )

    print("=== Successfully Wrote Snappy-Compressed Parquet ===")

    # 2. Read back Parquet: Schema and types are preserved in file metadata!
    parquet_df = spark.read.parquet("output/orders_snappy.parquet")

    print("=== Parquet Schema (Self-Describing) ===")
    parquet_df.printSchema()

    # 3. Predicate Pushdown: Parquet file footer stores min/max column values
    filtered_df = parquet_df.filter(col("amount") > 200.0)
    print("=== Filtered Parquet Scan (Min/Max Metadata Pushdown) ===")
    filtered_df.explain()
    filtered_df.show()

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson21: Lesson = {
  id: 121,
  title: 'Spark Lab 21 · Writing Efficient Parquet Files',
  concept: `**Apache Parquet** is the gold standard file format for analytics engineering and modern data lakes.

### Why Parquet Dominates CSV and JSON:
1. **Columnar Storage**: In a query like \`SELECT customer_id, SUM(amount)\`, Spark only reads the columns requested from disk! Non-projected columns are never read from storage.
2. **High Compression Ratio**: Homogeneous column data compresses dramatically better (often 80–90% space reduction with Snappy or Zstandard).
3. **Self-Describing Metadata**: Schema, data types, and column statistics (\`min\`, \`max\`, \`null_count\`) are stored directly in the file footer!
4. **Predicate Pushdown**: If a row group's metadata shows \`max(amount) = 150\`, a query with \`WHERE amount > 200\` **skips reading that file block entirely**!

### Writing Best Practices:
\`\`\`python
df.write.mode("overwrite").option("compression", "snappy").parquet("path/to/data")
\`\`\`

In this lab, you will write a DataFrame to Parquet using Snappy compression, inspect the self-describing schema, and trace metadata predicate pushdown.`,
  initialFiles: {
    'parquet_io.py': PARQUET_IO_PY,
    'data/orders.csv': ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_parquet_code',
      prompt: "Review `parquet_io.py` to confirm the use of `.parquet()` and `snappy` compression.",
      hint: "Check that `.parquet(...)` is called on the write stream with snappy compression.",
      validate: (s) =>
        fileContains(s, 'parquet_io.py', 'parquet') &&
        fileContains(s, 'parquet_io.py', 'snappy'),
    },
    {
      id: 'run_parquet_job',
      prompt: 'Execute the Parquet pipeline: `spark-submit parquet_io.py`.',
      hint: 'Type `spark-submit parquet_io.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('parquet_io') ||
        Boolean(s.lastRun?.args?.includes('parquet_io.py')),
    },
  ],
  quiz: {
    question: 'How does Parquet enable "Predicate Pushdown" during query execution?',
    options: [
      'It translates all queries to HTML',
      'The file footer contains min/max statistics for each column block, allowing Spark to skip entire row groups without reading them',
      'It reboots the executor cluster',
      'It requires all data to be unique',
    ],
    correctIndex: 1,
    explanation: 'Parquet files store statistical summaries (minimum, maximum, null counts) per row group. Spark checks these before reading actual data, skipping blocks that cannot contain matching rows.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'csv_input', label: 'orders.csv', layer: 'source' },
        { id: 'parquet_writer', label: 'Parquet Snappy Writer', layer: 'staging' },
        { id: 'predicate_scan', label: 'Predicate Pushdown Scan', layer: 'mart' },
      ],
      edges: [
        { source: 'csv_input', target: 'parquet_writer' },
        { source: 'parquet_writer', target: 'predicate_scan' },
      ],
    },
  },
  furtherReading: [
    { label: 'Apache Parquet Official Documentation', url: 'https://parquet.apache.org/' },
    { label: 'Spark SQL Parquet Files Guide', url: 'https://spark.apache.org/docs/latest/sql-data-sources-parquet.html' },
  ],
}

export default sparkLesson21
