import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { USERS_CSV } from './_canonical'

const DATAFRAMES_PY = `"""
Lab 3: DataFrames & Schema Inference
Load structured data into Spark DataFrames and inspect columns & types.
"""

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("DataFrameBasics").getOrCreate()

    # 1. Read CSV with header and inferSchema enabled
    users_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/users.csv")
    )

    print("=== DataFrame Schema ===")
    users_df.printSchema()

    print("=== First 5 Records ===")
    users_df.show(5)

    print(f"Total Rows: {users_df.count()}")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson03: Lesson = {
  id: 103,
  title: 'Spark Lab 3 · DataFrames & Schema Inference',
  concept: `While RDDs operate on opaque Python objects without schema awareness, **Spark DataFrames** represent distributed collections of data organized into named columns and typed schemas (like a relational table or Pandas DataFrame, but distributed across a cluster).

Key benefits of DataFrames over raw RDDs:
1. **The Catalyst Optimizer**: Spark can optimize query plans, push down filters, and reorder joins.
2. **Project Tungsten**: In-memory binary row encoding avoiding JVM object overhead and Garbage Collection.
3. **Structured APIs**: Rich SQL functions for aggregation, filtering, and joining.

Reading structured files with schema inference:
\`\`\`python
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/users.csv")
)
df.printSchema()
df.show(5)
\`\`\``,
  initialFiles: {
    'dataframes.py': DATAFRAMES_PY,
    'data/users.csv': USERS_CSV,
  },
  tasks: [
    {
      id: 'verify_read',
      prompt: "Inspect `dataframes.py` and verify `spark.read.option('header', 'true').option('inferSchema', 'true')`.",
      hint: "Check that header and inferSchema options are set on spark.read.",
      validate: (s) =>
        fileContains(s, 'dataframes.py', 'inferSchema') &&
        fileContains(s, 'dataframes.py', 'printSchema'),
    },
    {
      id: 'run_dataframe',
      prompt: 'Execute the DataFrame loader: `spark-submit dataframes.py`.',
      hint: 'Type `spark-submit dataframes.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('dataframes') ||
        Boolean(s.lastRun?.args?.includes('dataframes.py')),
    },
  ],
  quiz: {
    question: 'Why are DataFrames significantly faster and more memory-efficient than raw RDDs in PySpark?',
    options: [
      'DataFrames disable network communication entirely',
      'The Catalyst Optimizer optimizes query plans, and Tungsten encodes data into off-heap binary format',
      'DataFrames execute exclusively on local hard drives',
      'DataFrames convert everything into raw strings',
    ],
    correctIndex: 1,
    explanation: 'DataFrames understand column types and schema metadata, enabling the Catalyst optimizer to generate optimized bytecode and Project Tungsten to eliminate Python object serialization overhead.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'csv_file', label: 'data/users.csv', layer: 'source' },
        { id: 'inferred_schema', label: 'Schema Inference', layer: 'staging' },
        { id: 'dataframe', label: 'Spark DataFrame (users_df)', layer: 'mart' },
      ],
      edges: [
        { source: 'csv_file', target: 'inferred_schema' },
        { source: 'inferred_schema', target: 'dataframe' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark DataFrame API', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html' },
  ],
}

export default sparkLesson03
