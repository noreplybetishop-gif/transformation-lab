import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { USERS_CSV } from './_canonical'

const PARTITIONING_PY = `"""
Lab 19: Partitioning & Bucketing for Queries
Optimize physical storage layouts to prune file scans and speed up downstream queries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("PartitioningLab").getOrCreate()

    users_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv")

    print(f"Original Partitions: {users_df.rdd.getNumPartitions()}")

    # 1. Write partitioned by 'country'
    # Creates directory hierarchy: output/users_by_country/country=US/part-*.parquet
    (
        users_df
        .write
        .mode("overwrite")
        .partitionBy("country")
        .parquet("output/users_by_country")
    )

    print("=== Successfully Wrote Partitioned Dataset ===")

    # 2. Partition Pruning: Reading with a filter on the partition column
    # Spark only scans the directory country=US, skipping all other country files!
    us_users = (
        spark.read
        .parquet("output/users_by_country")
        .filter(col("country") == "US")
    )

    print("=== Partition Pruning Execution Plan ===")
    us_users.explain()
    us_users.show()

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson19: Lesson = {
  id: 119,
  title: 'Spark Lab 19 · Partitioning & Bucketing for Queries',
  concept: `How you organize data on disk directly impacts query performance. Two primary storage techniques in Apache Spark:

### 1. Partitioning (\`partitionBy\`)
Divides data into separate directory structures based on column values:
\`\`\`
output/
  country=US/
    part-00000.parquet
  country=CA/
    part-00001.parquet
\`\`\`
- **Partition Pruning**: When downstream queries filter \`WHERE country = 'US'\`, Spark skips scanning all other directories entirely!
- **Rule of thumb**: Partition columns should have low-to-medium cardinality (e.g. date, country, department) — never high cardinality like \`user_id\` or \`uuid\`, which leads to the "small files problem".

### 2. Bucketing (\`bucketBy\`)
Hashes records into a fixed number of buckets within files:
\`\`\`python
df.write.bucketBy(8, "user_id").sortBy("created_at").saveAsTable("bucketed_users")
\`\`\`
- Enables pre-shuffled joins without a runtime shuffle exchange between similarly bucketed tables.

In this lab, you will partition a dataset by country and observe partition pruning in the physical query plan.`,
  initialFiles: {
    'partitioning.py': PARTITIONING_PY,
    'data/users.csv': USERS_CSV,
  },
  tasks: [
    {
      id: 'verify_partitioning_code',
      prompt: "Inspect `partitioning.py` to ensure `partitionBy('country')` is used in the write stream.",
      hint: "Check that `.partitionBy('country')` is called on the DataFrameWriter.",
      validate: (s) =>
        fileContains(s, 'partitioning.py', 'partitionBy') &&
        fileContains(s, 'partitioning.py', 'parquet'),
    },
    {
      id: 'run_partitioning_job',
      prompt: 'Execute the partitioning job: `spark-submit partitioning.py`.',
      hint: 'Type `spark-submit partitioning.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('partitioning') ||
        Boolean(s.lastRun?.args?.includes('partitioning.py')),
    },
  ],
  quiz: {
    question: 'What is the danger of partitioning a dataset by a column with millions of unique values (like order_id or email)?',
    options: [
      'Data will be encrypted automatically',
      'The "Small Files Problem": Spark creates millions of tiny files and directories, overwhelming filesystem metadata and slowing reads',
      'Spark cannot write Parquet files with more than 10 columns',
      'It causes syntax errors in Python',
    ],
    correctIndex: 1,
    explanation: 'High-cardinality partitioning generates millions of tiny directories and files, exhausting OS inodes and catalog metadata memory while ruining scan efficiency.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw_users', label: 'users.csv', layer: 'source' },
        { id: 'partition_writer', label: 'partitionBy(country)', layer: 'staging' },
        { id: 'pruned_scan', label: 'Pruned Scan (country=US)', layer: 'mart' },
      ],
      edges: [
        { source: 'raw_users', target: 'partition_writer' },
        { source: 'partition_writer', target: 'pruned_scan' },
      ],
    },
  },
  furtherReading: [
    { label: 'Spark DataFrameWriter partitionBy', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.partitionBy.html' },
  ],
}

export default sparkLesson19
