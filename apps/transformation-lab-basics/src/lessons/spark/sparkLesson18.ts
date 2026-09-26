import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV } from './_canonical'

const CACHING_PY = `"""
Lab 18: Caching Strategies: cache() vs persist()
Prevent redundant DAG recomputations by persisting reusable DataFrames in memory.
"""

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col, count, sum, avg

def main():
    spark = SparkSession.builder.appName("CachingLab").getOrCreate()

    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")

    # Filter completed orders (expensive initial stage)
    completed_orders = orders_df.filter(col("status") == "COMPLETED")

    # 1. Persist in memory with disk fallback if memory fills up
    # completed_orders.cache() is shorthand for persist(StorageLevel.MEMORY_ONLY)
    completed_orders.persist(StorageLevel.MEMORY_AND_DISK)

    # 2. First Action: Triggers DAG evaluation and saves partitions to executor cache
    total_completed = completed_orders.count()
    print(f"Total Completed Orders (Initial Cache Population): {total_completed}")

    # 3. Second Action: Reuses cached partitions without re-reading orders.csv or re-filtering!
    status_summary = (
        completed_orders
        .groupBy("status")
        .agg(sum("amount").alias("revenue"), avg("amount").alias("avg_check"))
    )
    print("=== Reusing Cached DataFrame for Aggregation ===")
    status_summary.show()

    # 4. Clean up memory when done
    completed_orders.unpersist()
    print("Cache successfully unpersisted.")

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson18: Lesson = {
  id: 118,
  title: 'Spark Lab 18 · Caching Strategies: cache() vs persist()',
  concept: `Because Spark uses lazy evaluation, every time you call an Action (\`count()\`, \`show()\`, \`collect()\`), Spark **re-evaluates the entire lineage graph from the raw source files** by default!

If your pipeline reads a large table, filters it, and then passes the result to three downstream reports, Spark will scan and filter the table **three separate times** unless you cache it!

### \`cache()\` vs \`persist()\`
- **\`df.cache()\`**: Stores the DataFrame in deserialized Java objects in memory (\`StorageLevel.MEMORY_ONLY\`).
- **\`df.persist(StorageLevel)\`**: Allows explicit control over storage level:
  - **\`MEMORY_AND_DISK\`**: Stores in memory, and spills partitions to executor disk if memory is full (safest for production).
  - **\`MEMORY_ONLY_SER\`**: Serialized in memory (reduces heap size, increases CPU slightly).
  - **\`DISK_ONLY\`**: Written purely to disk.

### Best Practices:
1. Only cache DataFrames that are reused multiple times in the same application.
2. Always call **\`df.unpersist()\`** when you are finished to release cluster memory for subsequent jobs.

In this lab, you will persist a filtered DataFrame using \`StorageLevel.MEMORY_AND_DISK\`, run multiple actions, and free memory with \`unpersist()\`.`,
  initialFiles: {
    'caching.py': CACHING_PY,
    'data/orders.csv': ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_caching_code',
      prompt: "Examine `caching.py` to confirm the use of `persist` with `StorageLevel.MEMORY_AND_DISK` and `unpersist()`.",
      hint: "Check that persist(StorageLevel.MEMORY_AND_DISK) and unpersist() are invoked.",
      validate: (s) =>
        fileContains(s, 'caching.py', 'persist') &&
        fileContains(s, 'caching.py', 'unpersist'),
    },
    {
      id: 'run_caching_job',
      prompt: 'Execute the caching pipeline: `spark-submit caching.py`.',
      hint: 'Type `spark-submit caching.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('caching') ||
        Boolean(s.lastRun?.args?.includes('caching.py')),
    },
  ],
  quiz: {
    question: 'When is calling df.cache() actually evaluated and loaded into executor memory?',
    options: [
      'Immediately on the line where df.cache() is called',
      'During code compilation before Python runs',
      'Lazily, only when the first Action (such as count or show) is executed on the DataFrame',
      'When the cluster is restarted',
    ],
    correctIndex: 2,
    explanation: 'Like transformations, cache() and persist() are lazy instructions. Spark only stores the partitions into memory when an Action forces the data through the execution pipeline.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'source', label: 'orders.csv Scan', layer: 'source' },
        { id: 'filter', label: 'filter(status == COMPLETED)', layer: 'staging' },
        { id: 'cache_node', label: 'persist(MEMORY_AND_DISK)', layer: 'mart' },
      ],
      edges: [
        { source: 'source', target: 'filter' },
        { source: 'filter', target: 'cache_node' },
      ],
    },
  },
  furtherReading: [
    { label: 'Spark Storage Levels Reference', url: 'https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence' },
  ],
}

export default sparkLesson18
