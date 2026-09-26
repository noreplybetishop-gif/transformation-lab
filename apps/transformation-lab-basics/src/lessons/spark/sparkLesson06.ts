import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV } from './_canonical'

const ACTIONS_PY = `"""
Lab 6: Common Actions: count, collect & take
Understand safe vs dangerous actions when retrieving distributed data to the driver.
"""

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("ActionsLab").getOrCreate()

    df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")

    # Action 1: count() - returns total row count (integer) to driver
    total_rows = df.count()
    print(f"Total Rows in Dataset: {total_rows}")

    # Action 2: take(n) - safely retrieves only the first N rows as a Python list
    sample_rows = df.take(3)
    print("=== Safe Sample with take(3) ===")
    for row in sample_rows:
        print(f"Order #{row['order_id']} - USD {row['amount']} ({row['status']})")

    # Action 3: show(n) - prints tabular representation to stdout
    print("=== Tabular Output with show() ===")
    df.show(3)

    # Note: collect() pulls the ENTIRE distributed dataset into Driver RAM!
    # Dangerous on 100GB+ datasets, leading to Driver OutOfMemoryError.

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson06: Lesson = {
  id: 106,
  title: 'Spark Lab 6 · Common Actions: count, collect & take',
  concept: `Actions trigger the evaluation of the Spark execution DAG and return results to the driver program or write data to external storage.

Common actions include:
- **\`count()\`**: Returns the total number of rows.
- **\`show(n)\`**: Formats and prints the top *n* rows directly to stdout in an ASCII table.
- **\`take(n)\`**: Safely retrieves an array of the first *n* Row objects to the driver.
- **\`first()\` / \`head()\`**: Retrieves the single first row.
- **\`collect()\`**: **CAUTION!** Retrieves **EVERY SINGLE ROW** across all cluster partitions into driver memory as a local Python list.

### The Danger of \`collect()\`
If your distributed dataset contains 100 million records (50 GB) and your driver node has 8 GB of RAM, calling \`df.collect()\` will instantly trigger a fatal **\`java.lang.OutOfMemoryError: Java heap space\`** and crash the driver.

Always prefer \`take(n)\`, \`limit(n)\`, or write to persistent storage!`,
  initialFiles: {
    'actions.py': ACTIONS_PY,
    'data/orders.csv': ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_actions',
      prompt: "Review `actions.py` to inspect `count()`, `take(3)`, and `show(3)`.",
      hint: "Check that count(), take(), and show() are used safely in actions.py.",
      validate: (s) =>
        fileContains(s, 'actions.py', 'count()') &&
        fileContains(s, 'actions.py', 'take('),
    },
    {
      id: 'run_actions',
      prompt: 'Execute the actions script: `spark-submit actions.py`.',
      hint: 'Run `spark-submit actions.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('actions') ||
        Boolean(s.lastRun?.args?.includes('actions.py')),
    },
  ],
  quiz: {
    question: 'Why should you avoid calling df.collect() on large production datasets in Spark?',
    options: [
      'It deletes all tables from the database',
      'It pulls all distributed rows across all executors onto the single driver machine, frequently causing Driver Out-Of-Memory (OOM) crashes',
      'It changes column names to uppercase',
      'It forces Spark to reboot the cluster',
    ],
    correctIndex: 1,
    explanation: 'collect() gathers every partition across all workers into driver process memory. On large datasets, this overwhelms driver RAM and crashes the entire application.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'distributed_data', label: 'Cluster Partitions (200 workers)', layer: 'source' },
        { id: 'action_filter', label: 'take(3) (Fetch 3 Rows)', layer: 'staging' },
        { id: 'driver_memory', label: 'Driver RAM (Safe)', layer: 'mart' },
      ],
      edges: [
        { source: 'distributed_data', target: 'action_filter' },
        { source: 'action_filter', target: 'driver_memory' },
      ],
    },
  },
  furtherReading: [
    { label: 'DataFrame Actions', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html#actions' },
  ],
}

export default sparkLesson06
