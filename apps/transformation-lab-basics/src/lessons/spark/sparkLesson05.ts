import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV } from './_canonical'

const LAZY_EVAL_PY = `"""
Lab 5: Lazy Evaluation in Practice
Observe how Spark constructs execution plans lazily until an Action triggers execution.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("LazyEvaluationLab").getOrCreate()

    # Step 1: Read CSV (builds Logical Plan; no cluster execution yet)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")

    # Step 2: Apply transformations (lazy; no rows are processed yet!)
    completed_orders = df.filter(col("status") == "COMPLETED")
    high_value_orders = completed_orders.filter(col("amount") > 100.0)

    print("=== Transformations Defined (No Data Evaluated Yet) ===")
    print("Action not yet called. Spark has only built an execution DAG.")

    # Step 3: Trigger Execution with an Action (count)
    total_high_value = high_value_orders.count()
    print(f"Action Executed! High-Value Completed Orders: {total_high_value}")

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson05: Lesson = {
  id: 105,
  title: 'Spark Lab 5 · Lazy Evaluation in Practice',
  concept: `One of Spark's most powerful architectural principles is **Lazy Evaluation**.

When you write transformations:
\`\`\`python
df2 = df.filter(col("status") == "COMPLETED")
df3 = df2.filter(col("amount") > 100.0)
df4 = df3.select("order_id", "amount")
\`\`\`
Spark **does not execute any calculations** immediately! It merely records your instructions as a Directed Acyclic Graph (DAG) of logical transformation steps.

### Why is Lazy Evaluation Beneficial?
1. **Whole-Query Optimization**: Rather than running multiple expensive passes over the data, the **Catalyst Optimizer** combines sequential filters, prunes unnecessary columns, and reorders joins before a single byte of data is read.
2. **Predicate Pushdown**: Spark can push filters down directly to storage layers (like Parquet or databases) so untouched rows are never even loaded into memory.

Computation only occurs when you call an **Action** (such as \`.count()\`, \`.show()\`, \`.collect()\`, or \`.write\`).`,
  initialFiles: {
    'lazy_eval.py': LAZY_EVAL_PY,
    'data/orders.csv': ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_lazy_script',
      prompt: "Review `lazy_eval.py` to identify the lazy transformation chain and the action (`.count()`).",
      hint: "Check that high_value_orders.count() is invoked at the end.",
      validate: (s) =>
        fileContains(s, 'lazy_eval.py', 'filter') &&
        fileContains(s, 'lazy_eval.py', 'count()'),
    },
    {
      id: 'run_lazy_eval',
      prompt: 'Execute the script: `spark-submit lazy_eval.py`.',
      hint: 'Type `spark-submit lazy_eval.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('lazy_eval') ||
        Boolean(s.lastRun?.args?.includes('lazy_eval.py')),
    },
  ],
  quiz: {
    question: 'At what point does Spark actually read data from storage and execute transformations?',
    options: [
      'The moment you write spark.read.csv(...)',
      'The moment you define df.filter(...)',
      'Only when an Action (such as count, show, or write) is called on the DataFrame',
      'When you close your IDE',
    ],
    correctIndex: 2,
    explanation: 'Spark is lazily evaluated: transformations only build a logical plan. Computation across cluster executors begins only when an action is triggered.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'orders_source', label: 'orders.csv', layer: 'source' },
        { id: 'logical_dag', label: 'Logical Plan (Lazy DAG)', layer: 'staging' },
        { id: 'action_trigger', label: 'Action: count() (Execution)', layer: 'mart' },
      ],
      edges: [
        { source: 'orders_source', target: 'logical_dag' },
        { source: 'logical_dag', target: 'action_trigger' },
      ],
    },
  },
  furtherReading: [
    { label: 'Transformations vs Actions', url: 'https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations' },
  ],
}

export default sparkLesson05
