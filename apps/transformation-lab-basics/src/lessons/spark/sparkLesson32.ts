import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { USERS_CSV, ORDERS_CSV } from './_canonical'

const SKEW_SALTING_PY = `"""
Lab 32: Data Skew Detection & Salt-Key Mitigation
Neutralize straggler tasks caused by skewed join keys with Salting techniques.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, floor, rand, explode, array

def main():
    spark = SparkSession.builder.appName("SkewSaltingMitigation").master("local[*]").getOrCreate()

    orders = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")
    users = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv")

    NUM_SALTS = 4

    # STEP 1: Salt the skewed fact table with a random integer in [0, NUM_SALTS - 1]
    salted_orders = orders.withColumn(
        "salted_user_id",
        concat(col("user_id"), lit("_"), floor(rand() * NUM_SALTS))
    )

    # STEP 2: Replicate the lookup dimension table for each salt factor using explode
    salt_array = array([lit(i) for i in range(NUM_SALTS)])
    replicated_users = (
        users
        .withColumn("salt", explode(salt_array))
        .withColumn("salted_user_id", concat(col("id"), lit("_"), col("salt")))
    )

    # STEP 3: Join on the uniformly distributed salted key
    balanced_join = (
        salted_orders
        .join(replicated_users, on="salted_user_id", how="inner")
        .select(
            salted_orders.order_id,
            salted_orders.amount,
            replicated_users.name.alias("user_name"),
            replicated_users.country
        )
    )

    print("=== BALANCED SALTED JOIN RESULTS ===")
    balanced_join.show(5)

    print(f"✓ Skew successfully mitigated across {NUM_SALTS} salt buckets.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson32: Lesson = {
  id: 132,
  title: 'Spark Lab 32 · Data Skew Detection & Salt-Key Mitigation',
  concept: `### The #1 Big Data Pipeline Killer: Data Skew
In distributed systems, work is partitioned by hashing the join or group-by key (\`hash(key) % numPartitions\`).

If 80% of your orders have \`user_id = null\` or belong to one celebrity/tenant account:
- 199 tasks finish in 5 seconds.
- 1 straggler task receives 100,000,000 rows, runs for 3 hours, spills gigabytes of memory to disk, and throws an OutOfMemoryError (OOM)!

### The Salt-Key Mitigation Technique
When Adaptive Query Execution (AQE) skew handling is not enough or when tuning legacy engines, **Salting** is the industry standard architectural pattern:

1. **Salt the Skewed Fact Table**:
   Append a pseudo-random integer suffix (e.g. \`0\` to \`3\`) to the join key:
   \`\`\`python
   salted_fact = fact.withColumn(
       "salted_key", 
       concat(col("key"), lit("_"), floor(rand() * 4))
   )
   \`\`\`
   This splits the single giant key into 4 distinct hash buckets, distributing the workload evenly across 4 cluster cores!

2. **Replicate the Dimension Table**:
   To ensure matches succeed, replicate the smaller lookup table by exploding an array of the same salt range \`[0, 1, 2, 3]\`, creating corresponding salted keys:
   \`\`\`python
   replicated_dim = dim.withColumn("salt", explode(array([lit(0), lit(1), lit(2), lit(3)]))) \\
                       .withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))
   \`\`\`

3. **Join on \`salted_key\`**:
   All executors finish in parallel without hot spots!`,
  initialFiles: {
    'skew_salting.py': SKEW_SALTING_PY,
    'data/orders.csv': ORDERS_CSV,
    'data/users.csv': USERS_CSV,
  },
  tasks: [
    {
      id: 'verify_salting_logic',
      prompt: "Ensure `skew_salting.py` creates `salted_user_id` using `concat` with random salt numbers.",
      hint: "Check that salted_orders adds the salted_user_id column.",
      validate: (s) =>
        fileContains(s, 'skew_salting.py', 'salted_user_id') &&
        fileContains(s, 'skew_salting.py', 'explode'),
    },
    {
      id: 'run_salting_script',
      prompt: 'Execute the salting pipeline: `spark-submit skew_salting.py`.',
      hint: 'Type `spark-submit skew_salting.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('skew_salting') ||
        Boolean(s.lastRun?.args?.includes('skew_salting.py')),
    },
  ],
  quiz: {
    question: 'Why does key salting eliminate straggler tasks during joins?',
    options: [
      'It discards all skewed rows so Spark does not have to compute them',
      'It distributes high-frequency identical keys across multiple hash buckets by appending pseudo-random suffixes',
      'It stops all worker nodes and forces the driver to run everything',
      'It converts CSV files into audio streams',
    ],
    correctIndex: 1,
    explanation: 'By appending a random salt to the fact key and duplicating matching dimension rows, identical keys are hashed to distinct worker partitions instead of piling up on a single executor.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'fact', label: 'Skewed Fact Table', layer: 'source' },
        { id: 'salt', label: 'Salted Keys (x4 Buckets)', layer: 'staging' },
        { id: 'balanced', label: 'Balanced Parallel Executors', layer: 'mart' },
      ],
      edges: [
        { source: 'fact', target: 'salt' },
        { source: 'salt', target: 'balanced' },
      ],
    },
  },
  furtherReading: [
    { label: 'Handling Data Skew in Apache Spark (Databricks)', url: 'https://www.databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html#skew' },
  ],
}

export default sparkLesson32
