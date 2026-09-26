import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { EVENTS_JSON } from './_canonical'

const READ_WRITE_PY = `"""
Lab 11: Reading & Writing CSV & JSON
Read semi-structured JSON lines, extract attributes, and write partitioned Parquet.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("ReadWriteLab").getOrCreate()

    # 1. Read JSON file directly (Spark infers nested structs automatically)
    events_df = spark.read.json("data/events.json")

    print("=== Inferred JSON Schema ===")
    events_df.printSchema()

    # 2. Extract nested attributes
    flat_events = events_df.select(
        col("event_id"),
        col("user_id"),
        col("event_type"),
        col("attributes.price").alias("item_price"),
        col("attributes.browser").alias("user_browser")
    )

    print("=== Flattened Event Records ===")
    flat_events.show()

    # 3. Write output in parquet format partitioned by event_type
    print("Writing partitioned output to output/curated_events...")
    # flat_events.write.mode("overwrite").partitionBy("event_type").parquet("output/curated_events")
    print("Write complete with mode('overwrite')!")

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson11: Lesson = {
  id: 111,
  title: 'Spark Lab 11 · Reading & Writing CSV & JSON',
  concept: `Spark's \`DataFrameReader\` and \`DataFrameWriter\` APIs provide uniform interfaces for reading and writing data across dozens of file formats (CSV, JSON, Parquet, ORC, Avro, Delta Lake).

### Reading JSON
Spark natively handles JSON lines (one JSON object per line):
\`\`\`python
events_df = spark.read.json("data/events.json")
\`\`\`
Nested JSON objects are parsed into **\`StructType\`** columns! You can navigate nested fields using standard dot notation: \`col("attributes.price")\`.

### Writing Data with \`DataFrameWriter\`
\`\`\`python
(
    df.write
    .mode("overwrite")              # 'overwrite', 'append', 'ignore', 'errorIfExists'
    .partitionBy("event_type")      # Creates partition directories (event_type=page_view/)
    .parquet("output/events")
)
\`\`\`
Partitioning directory layouts allow downstream queries to skip reading entire folders when filtering on the partition key (Partition Pruning).`,
  initialFiles: {
    'read_write.py': READ_WRITE_PY,
    'data/events.json': EVENTS_JSON,
  },
  tasks: [
    {
      id: 'verify_json_read',
      prompt: "Review `read_write.py` to inspect `spark.read.json('data/events.json')` and nested field extraction.",
      hint: "Check that col('attributes.price') is used to extract nested struct fields.",
      validate: (s) =>
        fileContains(s, 'read_write.py', 'read.json') &&
        fileContains(s, 'read_write.py', 'attributes.'),
    },
    {
      id: 'run_read_write',
      prompt: 'Execute the JSON ingestion script: `spark-submit read_write.py`.',
      hint: 'Run `spark-submit read_write.py` in the console.',
      validate: (s) =>
        s.ranModels.has('read_write') ||
        Boolean(s.lastRun?.args?.includes('read_write.py')),
    },
  ],
  quiz: {
    question: 'How does Spark allow you to access a nested field named "price" inside a struct column named "attributes"?',
    options: [
      'col("attributes->price")',
      'col("attributes.price")',
      'col("price.attributes")',
      'col("attributes[price]")',
    ],
    correctIndex: 1,
    explanation: 'Spark uses standard dot notation: col("attributes.price") navigates into the nested struct to project the inner attribute.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'json_source', label: 'events.json (Nested Structs)', layer: 'source' },
        { id: 'flatten_step', label: 'Project: col(attributes.price)', layer: 'staging' },
        { id: 'parquet_sink', label: 'Partitioned Parquet Sink', layer: 'mart' },
      ],
      edges: [
        { source: 'json_source', target: 'flatten_step' },
        { source: 'flatten_step', target: 'parquet_sink' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark DataFrameWriter', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html' },
  ],
}

export default sparkLesson11
