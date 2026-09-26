import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV } from './_canonical'

const AGGREGATIONS_PY = `"""
Lab 9: Grouping & Aggregate Functions
Compute distributed summaries using groupBy and agg().
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, round, min, max

def main():
    spark = SparkSession.builder.appName("AggregationsLab").getOrCreate()

    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")

    # 1. Simple GroupBy with shorthand agg
    status_summary = orders_df.groupBy("status").count()
    print("=== Order Count by Status ===")
    status_summary.show()

    # 2. Multi-Metric Aggregation using .agg()
    user_summary = (
        orders_df
        .filter(col("status") == "COMPLETED")
        .groupBy("user_id")
        .agg(
            count("order_id").alias("total_orders"),
            round(sum("amount"), 2).alias("total_spend"),
            round(avg("amount"), 2).alias("avg_order_value"),
            min("amount").alias("min_order"),
            max("amount").alias("max_order")
        )
        .orderBy(col("total_spend").desc())
    )

    print("=== Customer Spend Aggregations ===")
    user_summary.show()

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson09: Lesson = {
  id: 109,
  title: 'Spark Lab 9 · Grouping & Aggregate Functions',
  concept: `Computing distributed aggregates across billions of records requires **\`groupBy()\`** and **\`agg()\`**:

\`\`\`python
from pyspark.sql.functions import count, sum, avg, round

summary_df = (
    df.groupBy("user_id")
    .agg(
        count("order_id").alias("order_count"),
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("avg_amount")
    )
)
\`\`\`

### How Spark Executes GroupBy Under the Hood
1. **Map-Side Combine**: Each worker node locally aggregates rows matching the key within its own partition (e.g. summing local amounts). This drastically minimizes network data volume!
2. **Shuffle Exchange**: Partition keys are hashed and transferred to designated reducer nodes.
3. **Reduce-Side Aggregation**: Reducer nodes compute the final merged sum/average across all partition contributions.`,
  initialFiles: {
    'aggregations.py': AGGREGATIONS_PY,
    'data/orders.csv': ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_agg',
      prompt: "Review `aggregations.py` to inspect `groupBy('user_id')` and the multi-metric `agg(...)` block.",
      hint: "Check that sum, avg, and count are computed under agg().",
      validate: (s) =>
        fileContains(s, 'aggregations.py', 'groupBy') &&
        fileContains(s, 'aggregations.py', 'agg('),
    },
    {
      id: 'run_aggregations',
      prompt: 'Execute the aggregation script: `spark-submit aggregations.py`.',
      hint: 'Run `spark-submit aggregations.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('aggregations') ||
        Boolean(s.lastRun?.args?.includes('aggregations.py')),
    },
  ],
  quiz: {
    question: 'How does Spark optimize groupBy aggregations before transferring data across the network?',
    options: [
      'It discards duplicate keys without calculating anything',
      'It performs map-side partial aggregation locally on each worker partition before the shuffle',
      'It downloads the entire dataset to a spreadsheet',
      'It runs all jobs on a single thread',
    ],
    correctIndex: 1,
    explanation: 'Spark uses map-side combiners to partially aggregate values within local partitions first, minimizing the volume of data that must be serialized across the network during the shuffle phase.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'orders_df', label: 'orders.csv', layer: 'source' },
        { id: 'map_side', label: 'Local Map-Side Combine', layer: 'staging' },
        { id: 'shuffle_agg', label: 'Shuffle Exchange', layer: 'intermediate' },
        { id: 'final_agg', label: 'Final user_summary', layer: 'mart' },
      ],
      edges: [
        { source: 'orders_df', target: 'map_side' },
        { source: 'map_side', target: 'shuffle_agg' },
        { source: 'shuffle_agg', target: 'final_agg' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark GroupedData', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html' },
  ],
}

export default sparkLesson09
