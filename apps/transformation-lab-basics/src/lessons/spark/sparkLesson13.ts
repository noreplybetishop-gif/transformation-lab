import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV } from './_canonical'

const CHAINING_PY = `"""
Lab 13: PySpark Functional Syntax & Method Chaining
Write clean, maintainable PySpark pipelines using fluent method chaining.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when

def main():
    spark = SparkSession.builder.appName("MethodChainingLab").getOrCreate()

    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")

    # Clean multi-step pipeline wrapped in parentheses
    processed_orders = (
        orders_df
        .filter(col("status") != "CANCELLED")
        .withColumn("tax_amount", round(col("amount") * 0.07, 2))
        .withColumn("total_with_tax", round(col("amount") + col("tax_amount"), 2))
        .withColumn(
            "priority",
            when(col("total_with_tax") >= 150.0, "VIP")
            .otherwise("REGULAR")
        )
        .select("order_id", "user_id", "amount", "tax_amount", "total_with_tax", "priority")
        .orderBy(col("total_with_tax").desc())
    )

    print("=== Processed Orders (Chained Pipeline) ===")
    processed_orders.show(5)

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson13: Lesson = {
  id: 113,
  title: 'Spark Lab 13 · PySpark Functional Syntax & Method Chaining',
  concept: `Welcome to the **Spark Intermediate Course**!

In production data engineering, writing code with messy intermediate variables:
\`\`\`python
# Bad practice: verbose and error-prone
df1 = df.filter(...)
df2 = df1.withColumn(...)
df3 = df2.select(...)
\`\`\`
clutters namespace memory and is hard to refactor. Instead, production PySpark uses **Fluent Method Chaining** wrapped in outer parentheses:

\`\`\`python
# Best practice: functional, declarative, self-documenting
clean_df = (
    df
    .filter(col("status") == "COMPLETED")
    .withColumn("tax", round(col("amount") * 0.08, 2))
    .withColumn("total", col("amount") + col("tax"))
    .select("order_id", "total")
    .orderBy(col("total").desc())
)
\`\`\`
Because Spark evaluates lazily, the Catalyst optimizer treats this entire chained block as a single unified expression tree, optimizing execution in one pass!`,
  initialFiles: {
    'chaining.py': CHAINING_PY,
    'data/orders.csv': ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_chaining',
      prompt: "Review `chaining.py` to inspect the fluent chaining pattern with filter, withColumn, and select.",
      hint: "Check that transformations are chained inside outer parentheses.",
      validate: (s) =>
        fileContains(s, 'chaining.py', 'withColumn("tax_amount"') &&
        fileContains(s, 'chaining.py', 'withColumn("total_with_tax"'),
    },
    {
      id: 'run_chaining',
      prompt: 'Execute the chained pipeline: `spark-submit chaining.py`.',
      hint: 'Run `spark-submit chaining.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('chaining') ||
        Boolean(s.lastRun?.args?.includes('chaining.py')),
    },
  ],
  quiz: {
    question: 'Why is wrapping chained PySpark DataFrame transformations in outer parentheses considered best practice?',
    options: [
      'It allows multi-line formatting without messy line-continuation backslashes (\\) and makes git diffs easy to read',
      'It forces Spark to run on GPU hardware',
      'It prevents Python from creating variables in the global namespace',
      'It encrypts the source code',
    ],
    correctIndex: 0,
    explanation: 'Wrapping method chains in parentheses allows clean, multi-line declarative code formatting without backslashes, making code reviews and maintenance simple.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'orders_csv', label: 'orders.csv', layer: 'source' },
        { id: 'tax_calc', label: 'withColumn(tax_amount)', layer: 'staging' },
        { id: 'vip_tier', label: 'withColumn(priority)', layer: 'intermediate' },
        { id: 'ordered_sink', label: 'Final Ordered Sink', layer: 'mart' },
      ],
      edges: [
        { source: 'orders_csv', target: 'tax_calc' },
        { source: 'tax_calc', target: 'vip_tier' },
        { source: 'vip_tier', target: 'ordered_sink' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark Code Style Guide', url: 'https://spark.apache.org/docs/latest/api/python/index.html' },
  ],
}

export default sparkLesson13
