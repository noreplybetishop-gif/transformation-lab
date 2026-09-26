import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV } from './_canonical'

const COLUMNS_PY = `"""
Lab 8: Column Expressions & withColumn
Add computed columns, apply conditional expressions, and cast data types.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when

def main():
    spark = SparkSession.builder.appName("ColumnsLab").getOrCreate()

    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")

    # 1. Add calculated column: 10% discount amount
    with_discount = orders_df.withColumn(
        "discounted_amount",
        round(col("amount") * 0.9, 2)
    )

    # 2. Add conditional tier column using when / otherwise
    with_tier = with_discount.withColumn(
        "order_tier",
        when(col("amount") >= 200.0, "HIGH")
        .when(col("amount") >= 100.0, "MEDIUM")
        .otherwise("LOW")
    )

    # 3. Add boolean flag for completed orders
    enriched_df = with_tier.withColumn(
        "is_completed",
        col("status") == "COMPLETED"
    )

    print("=== Enriched Orders DataFrame ===")
    enriched_df.select("order_id", "amount", "discounted_amount", "order_tier", "is_completed").show(5)

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson08: Lesson = {
  id: 108,
  title: 'Spark Lab 8 · Column Expressions & withColumn',
  concept: `DataFrames are modified by adding, replacing, or renaming columns using **\`withColumn()\`**:

\`\`\`python
from pyspark.sql.functions import col, round, when

df2 = df.withColumn("tax", round(col("amount") * 0.08, 2))
\`\`\`
- First argument: The name of the column (string).
- Second argument: A \`Column\` expression.

### Conditional Logic with \`when()\` / \`otherwise()\`
Spark provides the equivalent of SQL's \`CASE WHEN ... THEN ... ELSE\`:
\`\`\`python
df3 = df2.withColumn(
    "tier",
    when(col("amount") >= 100, "GOLD")
    .when(col("amount") >= 50, "SILVER")
    .otherwise("STANDARD")
)
\`\`\`
**Note on Immutability**: Calling \`withColumn()\` does not mutate the existing DataFrame in place; it returns a new DataFrame referencing the augmented lineage.`,
  initialFiles: {
    'columns.py': COLUMNS_PY,
    'data/orders.csv': ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_columns',
      prompt: "Review `columns.py` to inspect `withColumn()` and `when().otherwise()` expressions.",
      hint: "Check that discounted_amount and order_tier are added with withColumn.",
      validate: (s) =>
        fileContains(s, 'columns.py', 'withColumn') &&
        fileContains(s, 'columns.py', 'when'),
    },
    {
      id: 'run_columns',
      prompt: 'Execute the column transformations: `spark-submit columns.py`.',
      hint: 'Run `spark-submit columns.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('columns') ||
        Boolean(s.lastRun?.args?.includes('columns.py')),
    },
  ],
  quiz: {
    question: 'What is returned when you invoke df.withColumn("new_col", ...) on a DataFrame?',
    options: [
      'An integer representing the new column position',
      'The original DataFrame is mutated in-place and nothing is returned',
      'A new DataFrame with the updated schema and lineage expression',
      'A raw Python dictionary',
    ],
    correctIndex: 2,
    explanation: 'DataFrames are immutable in Spark. Operations like withColumn() produce a new DataFrame with the column projection appended to the plan.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'orders_raw', label: 'orders.csv', layer: 'source' },
        { id: 'with_calc', label: 'withColumn(discounted_amount)', layer: 'staging' },
        { id: 'with_tier', label: 'withColumn(order_tier, is_completed)', layer: 'mart' },
      ],
      edges: [
        { source: 'orders_raw', target: 'with_calc' },
        { source: 'with_calc', target: 'with_tier' },
      ],
    },
  },
  furtherReading: [
    { label: 'pyspark.sql.functions.when', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.when.html' },
  ],
}

export default sparkLesson08
