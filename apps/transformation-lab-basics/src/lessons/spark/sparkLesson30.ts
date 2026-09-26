import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV, USERS_CSV } from './_canonical'

const AQE_TUNING_PY = `"""
Lab 30: Adaptive Query Execution (AQE) Deep Dive
Dynamically coalesce shuffle partitions and optimize joins at runtime.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def main():
    spark = (
        SparkSession.builder
        .appName("AQETuning")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .master("local[*]")
        .getOrCreate()
    )

    orders = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")
    users = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv")

    joined = (
        orders
        .join(users, orders.user_id == users.id, how="inner")
        .groupBy("country")
        .agg(count("order_id").alias("order_count"))
    )

    print("=== AQE RUNTIME EXECUTION ===")
    joined.show()

    # Verify AQE configuration state
    aqe_status = spark.conf.get("spark.sql.adaptive.enabled")
    coalesce_status = spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")
    print(f"AQE Enabled: {aqe_status}, Coalesce Partitions: {coalesce_status}")

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson30: Lesson = {
  id: 130,
  title: 'Spark Lab 30 · Adaptive Query Execution (AQE) Deep Dive',
  concept: `Before Spark 3.0, query plans were completely static. Spark had to guess shuffle partition numbers (defaulting to \`spark.sql.shuffle.partitions=200\`) based solely on catalog statistics, often creating hundreds of tiny, empty partitions or crashing on skewed keys.

**Adaptive Query Execution (AQE)** re-optimizes and re-plans the physical query DAG during execution using accurate runtime statistics gathered at shuffle stage boundaries!

### The Three Major AQE Capabilities

1. **Dynamically Coalescing Shuffle Partitions**:
   Instead of launching 200 tasks where each task reads only 1KB, AQE automatically combines small adjacent partitions into target partition sizes (e.g. 64MB).
   \`\`\`python
   spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
   \`\`\`

2. **Dynamically Converting Sort-Merge Join to Broadcast Hash Join**:
   If a filtered relation ends up smaller than the broadcast threshold (e.g. < 10MB) after earlier stages run, AQE converts an expensive Sort-Merge Join into a lightning-fast Broadcast Hash Join without code changes.

3. **Dynamically Handling Skew Joins**:
   If one partition is significantly larger than the median, AQE splits the skewed partition into smaller sub-partitions and duplicates matching records on the other side, preventing the "one slow task" problem.
   \`\`\`python
   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
   \`\`\``,
  initialFiles: {
    'aqe_tuning.py': AQE_TUNING_PY,
    'data/orders.csv': ORDERS_CSV,
    'data/users.csv': USERS_CSV,
  },
  tasks: [
    {
      id: 'verify_aqe_configs',
      prompt: "Ensure `aqe_tuning.py` enables `spark.sql.adaptive.enabled` and `spark.sql.adaptive.coalescePartitions.enabled`.",
      hint: "Verify both configurations are set to 'true' in the SparkSession builder.",
      validate: (s) =>
        fileContains(s, 'aqe_tuning.py', 'spark.sql.adaptive.enabled') &&
        fileContains(s, 'aqe_tuning.py', 'spark.sql.adaptive.coalescePartitions.enabled'),
    },
    {
      id: 'run_aqe_script',
      prompt: 'Execute the AQE pipeline: `spark-submit aqe_tuning.py`.',
      hint: 'Run `spark-submit aqe_tuning.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('aqe_tuning') ||
        Boolean(s.lastRun?.args?.includes('aqe_tuning.py')),
    },
  ],
  quiz: {
    question: 'When does Adaptive Query Execution (AQE) perform query re-planning?',
    options: [
      'During static code compilation before any tasks run',
      'At runtime at shuffle stage boundaries using actual intermediate partition sizes',
      'Only after the entire cluster shuts down',
      'Inside Python while reading files',
    ],
    correctIndex: 1,
    explanation: 'AQE pauses at shuffle stage boundaries (MapStage completions), inspects the exact data sizes written to disk, and dynamically adjusts downstream partition counts and join algorithms.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'stage0', label: 'Stage 0 (Map & Shuffle Write)', layer: 'source' },
        { id: 'aqe_stat', label: 'AQE Runtime Stats & Coalesce', layer: 'staging' },
        { id: 'stage1', label: 'Stage 1 (Optimized Reduce Tasks)', layer: 'mart' },
      ],
      edges: [
        { source: 'stage0', target: 'aqe_stat' },
        { source: 'aqe_stat', target: 'stage1' },
      ],
    },
  },
  furtherReading: [
    { label: 'Adaptive Query Execution: Speeding Up Spark SQL at Runtime', url: 'https://www.databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html' },
  ],
}

export default sparkLesson30
