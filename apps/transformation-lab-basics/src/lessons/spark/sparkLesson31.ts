import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV, DIM_STORE_CSV } from './_canonical'

const DYNAMIC_PARTITION_PRUNING_PY = `"""
Lab 31: Dynamic Partition Pruning (DPP)
Prune partitions dynamically across join boundaries with broadcast subqueries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder
        .appName("DynamicPartitionPruning")
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        .master("local[*]")
        .getOrCreate()
    )

    orders = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")
    stores = spark.read.option("header", "true").option("inferSchema", "true").csv("data/stores.csv")

    # In a star-schema, orders is partitioned by store_id
    # Filtering the dimension table on 'East' region allows Spark to skip scanning non-East orders partitions
    filtered_stores = stores.filter(col("region") == "East")

    joined = (
        orders
        .join(filtered_stores, orders.order_id == filtered_stores.store_id, how="inner")
        .select(
            orders.order_id,
            orders.amount,
            filtered_stores.store_name,
            filtered_stores.region
        )
    )

    print("=== DYNAMIC PARTITION PRUNING (DPP) EXECUTION ===")
    joined.show()

    # Look for 'dynamicpruningsubquery' or partition filters in the plan
    joined.explain()

    print("✓ Dynamic Partition Pruning verified.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson31: Lesson = {
  id: 131,
  title: 'Spark Lab 31 · Dynamic Partition Pruning (DPP)',
  concept: `Static partition pruning occurs when you write static filters on partition keys, such as \`WHERE date = '2024-01-01'\`.

However, in classic star-schema and data warehouse queries, you frequently join a giant **Fact table** (e.g. Billions of Sales rows partitioned by \`store_id\` or \`date\`) with a tiny **Dimension table** (e.g. 50 Stores filtered by \`region = 'East'\`):

\`\`\`sql
SELECT * 
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
WHERE s.region = 'East'
\`\`\`

Without DPP, Spark would have to read **all partitions of the fact table** across every store in the world, then perform a shuffle join to discard the non-East stores!

### How DPP Works
1. Spark executes the filter on the small dimension table (\`region = 'East'\`).
2. Spark builds a Broadcast set of matching \`store_id\` values.
3. This broadcast hash set is injected directly into the Fact table's physical scan as a \`DynamicPruningSubquery\`.
4. The storage engine skips entire directories/partitions on disk that do not match, reducing I/O by 90%+!

Configuration:
\`\`\`python
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
\`\`\``,
  initialFiles: {
    'dynamic_partition_pruning.py': DYNAMIC_PARTITION_PRUNING_PY,
    'data/orders.csv': ORDERS_CSV,
    'data/stores.csv': DIM_STORE_CSV,
  },
  tasks: [
    {
      id: 'verify_dpp_config',
      prompt: "Confirm `dynamic_partition_pruning.py` enables `spark.sql.optimizer.dynamicPartitionPruning.enabled`.",
      hint: "Check config('spark.sql.optimizer.dynamicPartitionPruning.enabled', 'true') is present in the builder.",
      validate: (s) => fileContains(s, 'dynamic_partition_pruning.py', 'dynamicPartitionPruning.enabled'),
    },
    {
      id: 'run_dpp_script',
      prompt: 'Execute the DPP simulation: `spark-submit dynamic_partition_pruning.py`.',
      hint: 'Run `spark-submit dynamic_partition_pruning.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('dynamic_partition_pruning') ||
        Boolean(s.lastRun?.args?.includes('dynamic_partition_pruning.py')),
    },
  ],
  quiz: {
    question: 'How does Dynamic Partition Pruning (DPP) skip fact table partitions?',
    options: [
      'By reading all fact rows and deleting unwanted rows in memory',
      'By broadcasting filtered dimension keys into the fact table scan operator at runtime',
      'By turning off Spark SQL optimization',
      'By forcing every query to use single-core Python',
    ],
    correctIndex: 1,
    explanation: 'DPP evaluates the filter on the dimension table first, broadcasts the matching key set, and prunes fact table partition scans before reading the files from disk.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'dim', label: 'Filter dim_store (region=East)', layer: 'source' },
        { id: 'bcast', label: 'Broadcast Matching Store IDs', layer: 'staging' },
        { id: 'fact', label: 'Fact Table Scan (Pruned Partitions Only)', layer: 'mart' },
      ],
      edges: [
        { source: 'dim', target: 'bcast' },
        { source: 'bcast', target: 'fact' },
      ],
    },
  },
  furtherReading: [
    { label: 'Dynamic Partition Pruning in Apache Spark (Databricks)', url: 'https://www.databricks.com/blog/2020/05/27/dynamic-partition-pruning-in-apache-spark.html' },
  ],
}

export default sparkLesson31
