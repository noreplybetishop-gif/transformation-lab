import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV, USERS_CSV } from './_canonical'

const CATALYST_INTERNALS_PY = `"""
Lab 28: The Catalyst Optimizer Internals
Deconstruct Spark execution query plans from AST to physical bytecode.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

def main():
    spark = (
        SparkSession.builder
        .appName("CatalystInternals")
        .master("local[*]")
        .getOrCreate()
    )

    orders = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")
    users = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv")

    # Build transformation pipeline
    transformed = (
        orders
        .filter(col("amount") > 50.0)
        .join(users, orders.user_id == users.id, how="inner")
        .groupBy("country")
        .agg(sum("amount").alias("total"))
    )

    print("=== EXTENDED EXECUTION PLAN (4 PHASES OF CATALYST) ===")
    # Extended explain prints: Parsed, Analyzed, Optimized Logical, and Physical Plans
    transformed.explain(extended=True)

    print("✓ Catalyst query plan generated successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson28: Lesson = {
  id: 128,
  title: 'Spark Lab 28 · The Catalyst Optimizer Internals',
  concept: `At the core of Apache Spark's DataFrame and Dataset performance is the **Catalyst Optimizer**. 

Catalyst is an extensible query optimization framework written in Scala using tree-manipulation rules. When you express DataFrame operations, Catalyst processes your query through four distinct stages:

\`\`\`
  Unresolved Logical Plan (AST)
            │
            ▼ [Analysis: Catalog Resolution, Type Checking]
    Analyzed Logical Plan
            │
            ▼ [Optimization: Constant Folding, Predicate Pushdown, Projection Pruning]
   Optimized Logical Plan
            │
            ▼ [Physical Planning: Cost-Based Optimizer (CBO), Strategy Selection]
     Physical Plan (WholeStageCodegen, HashAggregate, BroadcastExchange)
\`\`\`

### The Four Stages of Catalyst
1. **Parsed Logical Plan (Unresolved)**: An Abstract Syntax Tree (AST) representing the raw query. Column references and table names are unchecked.
2. **Analyzed Logical Plan**: Resolves names and types against the Spark Catalog (verifying whether columns exist and types match).
3. **Optimized Logical Plan**: Applies standard relational algebra optimizations such as:
   - **Predicate Pushdown**: Filters are pushed as close to the data source as possible.
   - **Projection Pruning**: Unused columns are dropped before disk reading.
   - **Constant Folding**: Computations on constants like \`1 + 1\` are evaluated at plan time.
4. **Physical Plan**: Selects the physical operators (e.g. BroadcastHashJoin vs SortMergeJoin) and generates JVM bytecode via Whole-Stage Code Generation.

Use \`df.explain(extended=True)\` to inspect all four layers of the Catalyst plan!`,
  initialFiles: {
    'catalyst_internals.py': CATALYST_INTERNALS_PY,
    'data/orders.csv': ORDERS_CSV,
    'data/users.csv': USERS_CSV,
  },
  tasks: [
    {
      id: 'verify_explain_extended',
      prompt: "Ensure `catalyst_internals.py` invokes `transformed.explain(extended=True)` to deconstruct all 4 plan phases.",
      hint: "Check that `.explain(extended=True)` is called on the transformed DataFrame.",
      validate: (s) => fileContains(s, 'catalyst_internals.py', 'explain(extended=True)'),
    },
    {
      id: 'run_catalyst_script',
      prompt: 'Execute the Catalyst query plan analysis: `spark-submit catalyst_internals.py`.',
      hint: 'Run `spark-submit catalyst_internals.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('catalyst_internals') ||
        Boolean(s.lastRun?.args?.includes('catalyst_internals.py')),
    },
  ],
  quiz: {
    question: 'Which Catalyst phase is responsible for pushing filters down to the data source and pruning unused columns?',
    options: [
      'Parsed Logical Plan',
      'Optimized Logical Plan',
      'Physical Plan',
      'JVM Garbage Collector',
    ],
    correctIndex: 1,
    explanation: 'The Optimized Logical Plan applies standard relational algebraic optimizations, including predicate pushdown and projection pruning, before selecting a physical execution strategy.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'orders', label: 'orders.csv', layer: 'source' },
        { id: 'users', label: 'users.csv', layer: 'source' },
        { id: 'catalyst', label: 'Catalyst (Logical -> Physical)', layer: 'staging' },
        { id: 'plan', label: 'Optimized Physical Plan', layer: 'mart' },
      ],
      edges: [
        { source: 'orders', target: 'catalyst' },
        { source: 'users', target: 'catalyst' },
        { source: 'catalyst', target: 'plan' },
      ],
    },
  },
  furtherReading: [
    { label: 'Deep Dive into Spark SQL Catalyst Optimizer (Databricks)', url: 'https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html' },
    { label: 'Apache Spark df.explain Documentation', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html' },
  ],
}

export default sparkLesson28
