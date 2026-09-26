import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV, USERS_CSV } from './_canonical'

const SORT_MERGE_JOIN_PY = `"""
Lab 17: Sort-Merge Joins & Key Partitioning
Understand how Spark performs large-scale distributed joins between massive datasets.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder
        .appName("SortMergeJoinLab")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable broadcast to force SMJ
        .getOrCreate()
    )

    users_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv")
    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")

    # Perform Sort-Merge Join on large datasets
    # Step 1: Shuffle Exchange hashes user_id across partitions
    # Step 2: Sort in-partition by user_id
    # Step 3: Merge matching sorted streams
    smj_df = (
        orders_df
        .join(users_df, orders_df["user_id"] == users_df["id"], how="inner")
        .select(
            orders_df["order_id"],
            users_df["name"].alias("customer_name"),
            users_df["country"],
            orders_df["amount"]
        )
    )

    print("=== Sort-Merge Join Physical Plan ===")
    smj_df.explain()

    print("=== Joined Records ===")
    smj_df.show(5)

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson17: Lesson = {
  id: 117,
  title: 'Spark Lab 17 · Sort-Merge Joins & Key Partitioning',
  concept: `When joining two **large** tables (where neither fits in memory for a broadcast join), Apache Spark defaults to the **Sort-Merge Join (SMJ)** algorithm.

### The Three Phases of Sort-Merge Join:
1. **Shuffle Phase**: Both tables are hashed by the join key and redistributed across the cluster into $N$ shuffle partitions (\`spark.sql.shuffle.partitions\`). All records with the same join key land on the exact same worker executor.
2. **Sort Phase**: Each executor sorts its local partitions in ascending order by the join key.
3. **Merge Phase**: An iterator walks both sorted partitions simultaneously, matching keys in $O(N)$ linear time without loading all rows into RAM!

### Key Characteristics:
- **Robustness**: Does not require the dataset to fit in executor memory; handles spills to disk gracefully.
- **Cost**: Requires heavy network I/O during the shuffle phase and disk I/O during the sorting phase.

In this lab, you will disable broadcast joins (\`autoBroadcastJoinThreshold = -1\`), execute a Sort-Merge Join, and trace the \`SortMergeJoin\` operator in the query plan.`,
  initialFiles: {
    'sort_merge_join.py': SORT_MERGE_JOIN_PY,
    'data/users.csv': USERS_CSV,
    'data/orders.csv': ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_smj_config',
      prompt: "Review `sort_merge_join.py` to confirm `spark.sql.autoBroadcastJoinThreshold` is disabled and join condition is configured.",
      hint: "Check that autoBroadcastJoinThreshold is set to '-1' on the SparkSession builder.",
      validate: (s) =>
        fileContains(s, 'sort_merge_join.py', 'autoBroadcastJoinThreshold') &&
        fileContains(s, 'sort_merge_join.py', 'join'),
    },
    {
      id: 'run_smj_job',
      prompt: 'Run the sort-merge join application: `spark-submit sort_merge_join.py`.',
      hint: 'Type `spark-submit sort_merge_join.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('sort_merge_join') ||
        Boolean(s.lastRun?.args?.includes('sort_merge_join.py')),
    },
  ],
  quiz: {
    question: 'Why does Sort-Merge Join scale efficiently to terabyte and petabyte datasets where Hash Join fails?',
    options: [
      'It converts data to compressed MP3 files',
      'It does not require loading either table entirely into memory; matching keys are merged linearly from sorted streams',
      'It bypasses the Spark Catalyst optimizer',
      'It runs only on single CPU cores',
    ],
    correctIndex: 1,
    explanation: 'Sort-Merge Join sorts partitions on disk and streams matching rows sequentially, so it can join tables far larger than the cluster RAM without running out of memory.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'left_shuffle', label: 'Exchange hashpartitioning(user_id)', layer: 'source' },
        { id: 'right_shuffle', label: 'Exchange hashpartitioning(id)', layer: 'staging' },
        { id: 'sort_merge', label: 'SortMergeJoin [user_id == id]', layer: 'mart' },
      ],
      edges: [
        { source: 'left_shuffle', target: 'sort_merge' },
        { source: 'right_shuffle', target: 'sort_merge' },
      ],
    },
  },
  furtherReading: [
    { label: 'Deep Dive into Spark Sort-Merge Join', url: 'https://spark.apache.org/docs/latest/sql-performance-tuning.html' },
  ],
}

export default sparkLesson17
