import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { WEB_LOGS_CSV } from './_canonical'

const PARTITION_TUNING_PY = `"""
Lab 24: Repartition vs Coalesce
Master partition consolidation and rebalancing without unnecessary network shuffles.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("PartitionTuningLab").getOrCreate()

    logs_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/web_logs.csv")
    initial_partitions = logs_df.rdd.getNumPartitions()
    print(f"Initial Partitions: {initial_partitions}")

    # 1. repartition(n): WIDE transformation (Full Shuffle)
    # Rebalances data evenly across all executors, or increases partition count
    expanded_df = logs_df.repartition(4)
    print(f"After repartition(4): {expanded_df.rdd.getNumPartitions()} partitions (Requires Shuffle)")

    # 2. coalesce(n): NARROW transformation (No Full Shuffle)
    # Efficiently consolidates partitions DOWN (e.g. from 4 to 1) by merging adjacent partition blocks
    consolidated_df = expanded_df.coalesce(1)
    print(f"After coalesce(1): {consolidated_df.rdd.getNumPartitions()} partition (No Shuffle!)")

    print("=== Execution Plan for coalesce (Observe Absence of Shuffle Exchange) ===")
    consolidated_df.explain()

    print(f"Total Rows Preserved: {consolidated_df.count()}")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson24: Lesson = {
  id: 124,
  title: 'Spark Lab 24 · Repartition vs Coalesce',
  concept: `Managing partition count is one of the highest-leverage performance optimizations in Apache Spark:
- **Too many small partitions**: High scheduling overhead and the "small files problem".
- **Too few large partitions**: Underutilized CPU cores and potential Out-Of-Memory (OOM) memory spills.

### \`repartition(n)\` vs \`coalesce(n)\`

| Operation | Type | Network Shuffle? | Direction | Use Case |
|---|---|---|---|---|
| **\`repartition(n)\`** | Wide | **YES (Full Shuffle)** | Increase or Decrease | Rebalancing skewed keys evenly across workers; increasing parallelism |
| **\`coalesce(n)\`** | Narrow | **NO (Zero Shuffle)** | **Decrease ONLY** | Consolidating partitions before writing to avoid small files |

### Golden Rule of Production Writing:
Never write 200 tiny files after a filter or aggregation! Call \`.coalesce(1)\` or \`.coalesce(num_target_files)\` before calling \`.write\` to consolidate output files without paying for another shuffle!

In this lab, you will compare \`repartition\` against \`coalesce\` and inspect the resulting execution plans.`,
  initialFiles: {
    'partition_tuning.py': PARTITION_TUNING_PY,
    'data/web_logs.csv': WEB_LOGS_CSV,
  },
  tasks: [
    {
      id: 'verify_coalesce_code',
      prompt: "Inspect `partition_tuning.py` to confirm the use of both `repartition()` and `coalesce()`.",
      hint: "Check that `repartition(4)` and `coalesce(1)` are called on the DataFrame.",
      validate: (s) =>
        fileContains(s, 'partition_tuning.py', 'repartition') &&
        fileContains(s, 'partition_tuning.py', 'coalesce'),
    },
    {
      id: 'run_partition_tuning_job',
      prompt: 'Execute the partition tuning job: `spark-submit partition_tuning.py`.',
      hint: 'Run `spark-submit partition_tuning.py` in the console.',
      validate: (s) =>
        s.ranModels.has('partition_tuning') ||
        Boolean(s.lastRun?.args?.includes('partition_tuning.py')),
    },
  ],
  quiz: {
    question: 'When reducing the number of partitions from 100 to 10 before saving to disk, why should you prefer coalesce(10) over repartition(10)?',
    options: [
      'coalesce sorts the rows alphabetically',
      'coalesce collapses existing adjacent partitions locally without a costly full-cluster shuffle exchange',
      'repartition only works with CSV files',
      'coalesce deletes duplicate rows automatically',
    ],
    correctIndex: 1,
    explanation: 'coalesce avoids a network shuffle by merging adjacent partitions already on the same or neighboring nodes, saving significant network and disk I/O.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'input_partitions', label: 'Input Partitions', layer: 'source' },
        { id: 'repartition_shuffle', label: 'repartition(4) [Wide Shuffle]', layer: 'staging' },
        { id: 'coalesce_narrow', label: 'coalesce(1) [Narrow Merge]', layer: 'mart' },
      ],
      edges: [
        { source: 'input_partitions', target: 'repartition_shuffle' },
        { source: 'repartition_shuffle', target: 'coalesce_narrow' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark DataFrame.coalesce', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.coalesce.html' },
    { label: 'PySpark DataFrame.repartition', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html' },
  ],
}

export default sparkLesson24
