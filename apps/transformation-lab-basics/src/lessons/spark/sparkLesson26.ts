import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { USERS_CSV, ORDERS_CSV, PRODUCTS_CSV } from './_canonical'

const MULTI_JOIN_PY = `"""
Lab 26: Multi-Dataset Joins & Complex Lineages
Safely orchestrate multi-way joins with table aliasing to prevent column ambiguity.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum, broadcast

def main():
    spark = SparkSession.builder.appName("MultiJoinLab").getOrCreate()

    users_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv").alias("u")
    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv").alias("o")
    products_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/products.csv").alias("p")

    # Synthesize product_id on orders for relational join
    fact_orders = orders_df.withColumn("product_id", (col("order_id") % 6) + 101).alias("o")

    # Multi-way Join: Orders -> Users -> Products
    # Disambiguate column names using explicit table prefixes
    unified_analytics = (
        fact_orders
        .join(users_df, col("o.user_id") == col("u.id"), how="inner")
        .join(broadcast(products_df), col("o.product_id") == col("p.product_id"), how="left")
        .select(
            col("o.order_id"),
            col("u.name").alias("customer_name"),
            col("u.country"),
            col("p.name").alias("product_name"),
            col("p.category"),
            col("o.amount")
        )
    )

    print("=== Multi-Way Unified Customer Order Lineage ===")
    unified_analytics.explain()

    print("=== Final Unified Mart Sample ===")
    unified_analytics.show(5)

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson26: Lesson = {
  id: 126,
  title: 'Spark Lab 26 · Multi-Dataset Joins & Complex Lineages',
  concept: `Real enterprise pipelines rarely stop at a single two-table join. You often need to join facts with multiple dimension entities (e.g. Orders + Users + Products + Stores).

### The Danger of Column Ambiguity:
When multiple DataFrames share common column names (like \`id\`, \`name\`, \`created_at\`), calling \`col("name")\` post-join throws an **\`AnalysisException: Reference 'name' is ambiguous\`**.

### Best Practices for Multi-Way Joins:
1. **Assign Aliases**: Tag each DataFrame before joining:
\`\`\`python
users = users_df.alias("u")
orders = orders_df.alias("o")
products = products_df.alias("p")
\`\`\`
2. **Explicit Column References**:
\`\`\`python
joined = orders.join(users, col("o.user_id") == col("u.id")).select(col("u.name"), col("o.amount"))
\`\`\`
3. **Mix Join Strategies**: Use standard Sort-Merge Join for large-to-large facts, and \`broadcast()\` for small lookup tables in the same query graph!

In this lab, you will coordinate a 3-way join between Orders, Users, and Products with clean column aliasing.`,
  initialFiles: {
    'multi_join.py': MULTI_JOIN_PY,
    'data/users.csv': USERS_CSV,
    'data/orders.csv': ORDERS_CSV,
    'data/products.csv': PRODUCTS_CSV,
  },
  tasks: [
    {
      id: 'verify_multi_join_code',
      prompt: "Examine `multi_join.py` to confirm the use of DataFrame `.alias()` and multi-table join conditions.",
      hint: "Check that `.alias('u')`, `.alias('o')`, and `.alias('p')` are applied.",
      validate: (s) =>
        fileContains(s, 'multi_join.py', 'alias') &&
        fileContains(s, 'multi_join.py', 'join'),
    },
    {
      id: 'run_multi_join_job',
      prompt: 'Execute the multi-way join pipeline: `spark-submit multi_join.py`.',
      hint: 'Type `spark-submit multi_join.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('multi_join') ||
        Boolean(s.lastRun?.args?.includes('multi_join.py')),
    },
  ],
  quiz: {
    question: 'How do you avoid the "AnalysisException: Reference column is ambiguous" error when joining tables with identical column names?',
    options: [
      'Rename all columns to numbers',
      'Use DataFrame aliases (e.g. df.alias("a")) and reference columns explicitly with col("a.col_name")',
      'Turn off type checking in Python',
      'Delete the columns before joining',
    ],
    correctIndex: 1,
    explanation: 'Using .alias() on each DataFrame allows referencing qualified paths like col("u.name") vs col("p.name"), resolving ambiguity cleanly in the Catalyst query analyzer.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'orders_node', label: 'orders_df.alias("o")', layer: 'source' },
        { id: 'users_node', label: 'users_df.alias("u")', layer: 'source' },
        { id: 'products_node', label: 'broadcast(products_df).alias("p")', layer: 'source' },
        { id: 'unified_mart', label: 'Unified 3-Way Analytics Mart', layer: 'mart' },
      ],
      edges: [
        { source: 'orders_node', target: 'unified_mart' },
        { source: 'users_node', target: 'unified_mart' },
        { source: 'products_node', target: 'unified_mart' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark DataFrame.alias', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.alias.html' },
  ],
}

export default sparkLesson26
