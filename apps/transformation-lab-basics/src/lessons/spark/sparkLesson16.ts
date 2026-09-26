import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV, PRODUCTS_CSV } from './_canonical'

const BROADCAST_JOIN_PY = `"""
Lab 16: Join Strategies: Broadcast Hash Joins
Use broadcast() hints to eliminate shuffle overhead when joining facts with dimensions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, round

def main():
    spark = SparkSession.builder.appName("BroadcastJoinLab").getOrCreate()

    # Load large fact table (Orders) and small dimension lookup (Products)
    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")
    products_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/products.csv")

    # 1. Synthesize matching product foreign keys for demonstration
    fact_orders = orders_df.withColumn("product_id", (col("order_id") % 6) + 101)

    # 2. Broadcast Join: The small products_df is copied to every executor,
    # completely bypassing the expensive Shuffle Exchange!
    enriched_orders = (
        fact_orders
        .join(broadcast(products_df), on="product_id", how="inner")
        .select(
            "order_id",
            "product_id",
            col("name").alias("product_name"),
            "category",
            "amount",
            "status"
        )
    )

    print("=== Broadcast Hash Join Execution Plan ===")
    enriched_orders.explain()

    print("=== Enriched Orders Sample ===")
    enriched_orders.show(5)

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson16: Lesson = {
  id: 116,
  title: 'Spark Lab 16 · Join Strategies: Broadcast Hash Joins',
  concept: `Joins in distributed systems are traditionally very slow because they trigger a **Shuffle Exchange**: both datasets must be re-partitioned across the network by join key so matching records end up on the same executor node.

### The Broadcast Hash Join (BHJ)
When one dataset is small enough (by default under 10MB in \`spark.sql.autoBroadcastJoinThreshold\`), Spark can **broadcast** the small table to every worker node in the cluster!

\`\`\`python
from pyspark.sql.functions import broadcast

# Tell Catalyst to broadcast the small dimension table
result = large_facts_df.join(broadcast(small_dim_df), on="id", how="inner")
\`\`\`

### Advantages:
1. **Zero Network Shuffle for Fact Table**: The multi-gigabyte fact table stays entirely in place on its local partition!
2. **Speed**: Often 10x to 100x faster than a Sort-Merge Join.
3. **No Skew Vulnerability**: Fact key skew does not overwhelm a single partition.

In this lab, you will apply the \`broadcast()\` hint and inspect the resulting physical execution plan.`,
  initialFiles: {
    'broadcast_join.py': BROADCAST_JOIN_PY,
    'data/orders.csv': ORDERS_CSV,
    'data/products.csv': PRODUCTS_CSV,
  },
  tasks: [
    {
      id: 'verify_broadcast_code',
      prompt: "Examine `broadcast_join.py` to confirm `broadcast(products_df)` is utilized in the join call.",
      hint: "Verify that `broadcast` is imported from `pyspark.sql.functions` and wraps `products_df`.",
      validate: (s) =>
        fileContains(s, 'broadcast_join.py', 'broadcast') &&
        fileContains(s, 'broadcast_join.py', 'broadcast(products_df)'),
    },
    {
      id: 'run_broadcast_job',
      prompt: 'Execute the broadcast join script: `spark-submit broadcast_join.py`.',
      hint: 'Type `spark-submit broadcast_join.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('broadcast_join') ||
        Boolean(s.lastRun?.args?.includes('broadcast_join.py')),
    },
  ],
  quiz: {
    question: 'Under what condition should you avoid using a Broadcast Hash Join?',
    options: [
      'When joining on integer columns',
      'When the broadcast table is too large and will cause Executor Out-Of-Memory (OOM) errors',
      'When running Spark in local mode',
      'When data is stored in CSV format',
    ],
    correctIndex: 1,
    explanation: 'Because broadcast sends a copy of the entire table into each executor memory space and driver memory, broadcasting large tables will crash your JVM executors with OutOfMemory errors.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'fact_orders', label: 'Fact Orders (Large)', layer: 'source' },
        { id: 'broadcast_products', label: 'Broadcast Products (Small)', layer: 'staging' },
        { id: 'bhj_result', label: 'BroadcastHashJoin (No Shuffle)', layer: 'mart' },
      ],
      edges: [
        { source: 'fact_orders', target: 'bhj_result' },
        { source: 'broadcast_products', target: 'bhj_result' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark broadcast documentation', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.broadcast.html' },
    { label: 'Spark SQL Performance Tuning - Join Strategies', url: 'https://spark.apache.org/docs/latest/sql-performance-tuning.html' },
  ],
}

export default sparkLesson16
