import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'

const RDD_BASICS_PY = `"""
Lab 2: RDD Fundamentals & Immutability
Create Resilient Distributed Datasets (RDDs) and perform transformations.
"""

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("RddBasics").getOrCreate()
    sc = spark.sparkContext

    # 1. Parallelize numbers 1 through 10 into an RDD with 2 partitions
    numbers = list(range(1, 11))
    numbers_rdd = sc.parallelize(numbers, numSlices=2)

    # 2. Narrow Transformation: filter for even numbers
    evens_rdd = numbers_rdd.filter(lambda x: x % 2 == 0)

    # 3. Narrow Transformation: square each number
    squared_rdd = evens_rdd.map(lambda x: x * x)

    # 4. Action: collect results back to driver
    results = squared_rdd.collect()
    print("Filtered & Squared Results:", results)
    print(f"Total Partitions: {squared_rdd.getNumPartitions()}")

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson02: Lesson = {
  id: 102,
  title: 'Spark Lab 2 · RDD Fundamentals & Immutability',
  concept: `The foundational abstraction of Apache Spark is the **Resilient Distributed Dataset (RDD)**:
- **Resilient**: Fault-tolerant through lineage graph recomputation. If an executor fails, Spark reconstructs lost partitions automatically.
- **Distributed**: Data is partitioned across multiple worker nodes.
- **Dataset**: A collection of partitioned records.

RDDs are **strictly immutable**. You never mutate an existing RDD in place; you apply transformations that yield new RDDs:
\`\`\`python
rdd = sc.parallelize([1, 2, 3, 4, 5])
even_rdd = rdd.filter(lambda x: x % 2 == 0)
squared_rdd = even_rdd.map(lambda x: x ** 2)
\`\`\`
In this lab, you will create an RDD, apply functional transformations (\`filter\`, \`map\`), and collect the results.`,
  initialFiles: {
    'rdd_basics.py': RDD_BASICS_PY,
  },
  tasks: [
    {
      id: 'verify_rdd_code',
      prompt: "Review `rdd_basics.py` to inspect `sc.parallelize` and the `filter` / `map` operations.",
      hint: "Check that numbers_rdd is filtered with lambda x: x % 2 == 0.",
      validate: (s) =>
        fileContains(s, 'rdd_basics.py', 'sc.parallelize') &&
        fileContains(s, 'rdd_basics.py', 'filter'),
    },
    {
      id: 'run_rdd',
      prompt: 'Execute the RDD script: `spark-submit rdd_basics.py`.',
      hint: 'Type `spark-submit rdd_basics.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('rdd_basics') ||
        Boolean(s.lastRun?.args?.includes('rdd_basics.py')),
    },
  ],
  quiz: {
    question: 'What does Spark do if a worker node crashes and loses an in-memory RDD partition?',
    options: [
      'The entire cluster shuts down permanently',
      'Spark uses the RDD lineage DAG graph to automatically recompute only the lost partition',
      'It asks the user to re-enter all data manually',
      'It returns null for all remaining calculations',
    ],
    correctIndex: 1,
    explanation: 'Because RDDs are deterministic and immutable, Spark records the exact transformation lineage graph, allowing it to rebuild any lost partition on a healthy worker.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'source_data', label: 'Raw Numbers [1..10]', layer: 'source' },
        { id: 'rdd_filtered', label: 'Filtered RDD (evens)', layer: 'staging' },
        { id: 'rdd_squared', label: 'Squared RDD [4, 16, 36...]', layer: 'mart' },
      ],
      edges: [
        { source: 'source_data', target: 'rdd_filtered' },
        { source: 'rdd_filtered', target: 'rdd_squared' },
      ],
    },
  },
  furtherReading: [
    { label: 'RDD Programming Guide', url: 'https://spark.apache.org/docs/latest/rdd-programming-guide.html' },
  ],
}

export default sparkLesson02
