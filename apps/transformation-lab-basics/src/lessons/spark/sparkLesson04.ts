import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { WEB_LOGS_CSV } from './_canonical'

const TRANSFORMATIONS_PY = `"""
Lab 4: Narrow vs Wide Transformations
Compare pipelined narrow transformations against wide transformations that trigger a shuffle.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("TransformationsLab").getOrCreate()

    logs_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/web_logs.csv")

    # 1. Narrow Transformation: filter()
    # Executed in-memory per partition without cross-network movement
    success_logs = logs_df.filter(col("status_code") == 200)

    # 2. Narrow Transformation: select()
    endpoint_ip = success_logs.select("endpoint", "ip_address")

    # 3. Wide Transformation: distinct()
    # Requires a Shuffle Exchange across cluster executors to deduplicate keys
    unique_visitors = endpoint_ip.distinct()

    print("=== Unique Endpoint Visitors (Post-Shuffle) ===")
    unique_visitors.show()

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson04: Lesson = {
  id: 104,
  title: 'Spark Lab 4 · Narrow vs Wide Transformations',
  concept: `In Apache Spark, transformations fall into two critical architectural categories:

### 1. Narrow Transformations (Pipelined)
Each input partition contributes to **at most one** output partition.
- Examples: \`filter()\`, \`map()\`, \`select()\`, \`withColumn()\`.
- **Performance**: Extremely fast. Operations are pipelined together into a single **Stage** in-memory without network transfers.

### 2. Wide Transformations (Shuffle)
Input partitions contribute to **many** output partitions.
- Examples: \`groupBy()\`, \`join()\`, \`distinct()\`, \`repartition()\`.
- **Performance**: High overhead. Requires a **Shuffle Exchange**, serializing data to disk and transferring it across the cluster network to regroup records by key.

Every Wide Transformation creates a **Stage Boundary** in your Spark execution DAG!`,
  initialFiles: {
    'transformations.py': TRANSFORMATIONS_PY,
    'data/web_logs.csv': WEB_LOGS_CSV,
  },
  tasks: [
    {
      id: 'verify_transformations',
      prompt: "Review `transformations.py` to identify narrow operations (`filter`, `select`) and the wide operation (`distinct`).",
      hint: "Check that filter and distinct are called on the DataFrame.",
      validate: (s) =>
        fileContains(s, 'transformations.py', 'filter') &&
        fileContains(s, 'transformations.py', 'distinct'),
    },
    {
      id: 'run_transformations',
      prompt: 'Execute the transformation pipeline: `spark-submit transformations.py`.',
      hint: 'Run `spark-submit transformations.py` in the console.',
      validate: (s) =>
        s.ranModels.has('transformations') ||
        Boolean(s.lastRun?.args?.includes('transformations.py')),
    },
  ],
  quiz: {
    question: 'Why does a wide transformation (like distinct or groupBy) cause a significant performance cost compared to a narrow filter?',
    options: [
      'It deletes files from the operating system',
      'It triggers a Shuffle Exchange: data must be sorted, written to disk, and transferred over the network between executors',
      'It converts data to XML format',
      'It stops the Spark driver process',
    ],
    correctIndex: 1,
    explanation: 'Wide transformations break stage boundaries because records sharing the same key must be grouped together onto the same worker node via network shuffling.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw_logs', label: 'Raw Logs', layer: 'source' },
        { id: 'stage_0', label: 'Stage 0 (Narrow: filter & select)', layer: 'staging' },
        { id: 'shuffle', label: 'Shuffle Exchange (Network Transfer)', layer: 'intermediate' },
        { id: 'stage_1', label: 'Stage 1 (Wide: distinct)', layer: 'mart' },
      ],
      edges: [
        { source: 'raw_logs', target: 'stage_0' },
        { source: 'stage_0', target: 'shuffle' },
        { source: 'shuffle', target: 'stage_1' },
      ],
    },
  },
  furtherReading: [
    { label: 'Spark Stages and Shuffling', url: 'https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations' },
  ],
}

export default sparkLesson04
