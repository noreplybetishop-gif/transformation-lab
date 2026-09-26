import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { USERS_CSV } from './_canonical'

const CUSTOM_PARTITIONERS_PY = `"""
Lab 42: Custom Partitioners & Salting Strategies
Control exact record routing across cluster nodes using custom partitioning.
"""

from pyspark.sql import SparkSession

def country_partitioner(country_key):
    """
    Custom Partitioner routing records by geographic region:
    0 -> North America (US, CA)
    1 -> Europe (UK, DE, FR)
    2 -> Asia-Pacific & Other (AU, etc.)
    """
    c = str(country_key).upper()
    if c in ("US", "CA"):
        return 0
    elif c in ("UK", "DE", "FR"):
        return 1
    else:
        return 2

def main():
    spark = SparkSession.builder.appName("CustomPartitioners").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    users_df = spark.read.option("header", "true").csv("data/users.csv")

    # Convert DataFrame to PairRDD: (country, (id, name))
    pair_rdd = users_df.rdd.map(lambda row: (row["country"], (row["id"], row["name"])))

    # Apply custom partitioner with 3 target partitions
    num_partitions = 3
    partitioned_rdd = pair_rdd.partitionBy(num_partitions, country_partitioner)

    # Inspect partition distributions using glom()
    glommed = partitioned_rdd.glom().collect()

    print("=== CUSTOM REGIONAL PARTITION DISTRIBUTION ===")
    for idx, partition_data in enumerate(glommed):
        print(f"Partition {idx} Count: {len(partition_data)} records")
        for k, v in partition_data[:2]:
            print(f"  • Country [{k}]: User {v[1]}")

    print("✓ Custom partitioner executed and verified.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson42: Lesson = {
  id: 142,
  title: 'Spark Lab 42 · Custom Partitioners & Salting Strategies',
  concept: `By default, Spark uses **HashPartitioner** (\`abs(hash(key)) % numPartitions\`) to divide data across tasks during wide shuffle operations.

While HashPartitioner is fine for uniformly distributed arbitrary keys, it is oblivious to domain-specific business topology.

### When to Use a Custom Partitioner?
1. **Multi-Tenant Isolation**: You want each enterprise customer/tenant or geographical region to reside in its own dedicated partition, preventing noisy-neighbor interference.
2. **Eliminating Cascading Shuffles**: If consecutive stages or joins share the same custom partitioning scheme, Spark's Catalyst optimizer detects that the child RDD/DataFrame is **already partitioned** and skips downstream network shuffles completely!

### Implementing a Custom Partitioner in PySpark
A custom partitioner is a Python function that accepts the key and returns an integer partition index \`0 <= index < numPartitions\`:

\`\`\`python
def my_partitioner(key):
    if key in ("US", "CA"): return 0
    if key in ("UK", "DE", "FR"): return 1
    return 2

# Apply to a PairRDD
pair_rdd.partitionBy(3, my_partitioner)
\`\`\`

Using \`.glom()\` allows you to inspect the list of elements grouped in each physical partition on worker executors.`,
  initialFiles: {
    'custom_partitioners.py': CUSTOM_PARTITIONERS_PY,
    'data/users.csv': USERS_CSV,
  },
  tasks: [
    {
      id: 'verify_custom_partitioner',
      prompt: "Ensure `custom_partitioners.py` implements `country_partitioner` and invokes `pair_rdd.partitionBy(3, country_partitioner)`.",
      hint: "Check that country_partitioner and partitionBy are present in the script.",
      validate: (s) =>
        fileContains(s, 'custom_partitioners.py', 'country_partitioner') &&
        fileContains(s, 'custom_partitioners.py', 'partitionBy'),
    },
    {
      id: 'run_partitioner_script',
      prompt: 'Execute the custom partitioner script: `spark-submit custom_partitioners.py`.',
      hint: 'Type `spark-submit custom_partitioners.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('custom_partitioners') ||
        Boolean(s.lastRun?.args?.includes('custom_partitioners.py')),
    },
  ],
  quiz: {
    question: 'What is a major performance benefit of applying an identical custom partitioner to two datasets before joining them?',
    options: [
      'Spark detects that matching keys already co-locate on the same nodes and skips the network shuffle completely',
      'It deletes all columns except the key',
      'It translates Python into assembly language',
      'It forces the driver to run everything single-threaded',
    ],
    correctIndex: 0,
    explanation: 'When two RDDs/DataFrames share the exact same partitioner and partition count, Spark recognizes co-partitioning and performs a narrow collocated join with zero network shuffle.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'pair', label: 'PairRDD (Country, User)', layer: 'source' },
        { id: 'part', label: 'Custom Regional Partitioner (0=NA, 1=EU, 2=APAC)', layer: 'staging' },
        { id: 'glom', label: 'glom() Co-located Partitions', layer: 'mart' },
      ],
      edges: [
        { source: 'pair', target: 'part' },
        { source: 'part', target: 'glom' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark RDD partitionBy Documentation', url: 'https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.partitionBy.html' },
  ],
}

export default sparkLesson42
