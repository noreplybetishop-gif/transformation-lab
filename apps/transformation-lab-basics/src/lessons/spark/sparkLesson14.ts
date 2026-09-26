import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { DAILY_SALES_CSV } from './_canonical'

const WINDOWING_PY = `"""
Lab 14: Advanced Analytical Window Functions
Compute running totals and rankings using PySpark Window specifications.
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, round, row_number, lag

def main():
    spark = SparkSession.builder.appName("WindowingLab").getOrCreate()

    sales_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/daily_sales.csv")

    # 1. Define Window Specification partitioned by store, ordered by date
    store_window = (
        Window
        .partitionBy("store_id")
        .orderBy("date")
    )

    # 2. Add Running Cumulative Total with unbounded preceding frame
    cumulative_window = store_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    windowed_df = (
        sales_df
        .withColumn("running_total", round(sum("sales_amount").over(cumulative_window), 2))
        .withColumn("rank_by_date", row_number().over(store_window))
        .withColumn("prev_day_sales", lag("sales_amount", 1).over(store_window))
    )

    print("=== Windowed Sales Analytics ===")
    windowed_df.show()

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson14: Lesson = {
  id: 114,
  title: 'Spark Lab 14 · Advanced Analytical Window Functions',
  concept: `Window functions compute calculations across a group of related rows while still returning a value **for each individual row** (unlike \`groupBy()\` which collapses rows).

Import \`Window\` from \`pyspark.sql.window\`:
\`\`\`python
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, row_number, lag

# Partition by customer and order by date
window_spec = Window.partitionBy("customer_id").orderBy("order_date")
\`\`\`

### Common Analytical Window Operations:
1. **Ranking**: \`row_number().over(window_spec)\`, \`rank()\`, \`dense_rank()\`.
2. **Lag / Lead**: \`lag("sales", 1).over(window_spec)\` retrieves previous record's value.
3. **Running Aggregates**:
\`\`\`python
running_spec = window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_revenue", sum("amount").over(running_spec))
\`\`\`
Spark evaluates window specifications efficiently by sorting within partition boundaries.`,
  initialFiles: {
    'windowing.py': WINDOWING_PY,
    'data/daily_sales.csv': DAILY_SALES_CSV,
  },
  tasks: [
    {
      id: 'verify_window',
      prompt: "Review `windowing.py` to inspect `Window.partitionBy('store_id').orderBy('date')`.",
      hint: "Check that Window.partitionBy and over() are used in windowing.py.",
      validate: (s) =>
        fileContains(s, 'windowing.py', 'Window.partitionBy') &&
        fileContains(s, 'windowing.py', 'rowsBetween'),
    },
    {
      id: 'run_window',
      prompt: 'Execute the windowing script: `spark-submit windowing.py`.',
      hint: 'Run `spark-submit windowing.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('windowing') ||
        Boolean(s.lastRun?.args?.includes('windowing.py')),
    },
  ],
  quiz: {
    question: 'How do Window functions differ from groupBy aggregations in PySpark?',
    options: [
      'Window functions collapse all rows into a single summary record',
      'Window functions compute analytical metrics over a window of rows while preserving individual input rows in the output',
      'Window functions can only run on local machines',
      'Window functions delete duplicate keys',
    ],
    correctIndex: 1,
    explanation: 'Unlike groupBy() which rolls up rows, Window functions preserve every original input row while appending contextual calculations (e.g. running totals, ranks, lags).',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'daily_sales', label: 'daily_sales.csv', layer: 'source' },
        { id: 'window_spec', label: 'Window.partitionBy(store_id)', layer: 'staging' },
        { id: 'window_sink', label: 'Running Totals & Lag', layer: 'mart' },
      ],
      edges: [
        { source: 'daily_sales', target: 'window_spec' },
        { source: 'window_spec', target: 'window_sink' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark Window Functions Guide', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html' },
  ],
}

export default sparkLesson14
