import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { USERS_CSV } from './_canonical'

const TIME_TRAVEL_PY = `"""
Lab 23: Time Travel & Table History in Delta Lake
Query historical table snapshots using versionAsOf and timestampAsOf.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder
        .appName("DeltaTimeTravelLab")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    delta_path = "output/delta_users_timetravel"

    # Step 1: Write initial version 0 (8 rows)
    initial_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv")
    initial_df.write.format("delta").mode("overwrite").save(delta_path)

    # Step 2: Write version 1 with an update (simulate accidental deletion or update)
    filtered_v1 = initial_df.filter(col("country") == "US")
    filtered_v1.write.format("delta").mode("overwrite").save(delta_path)

    print("=== Current Table State (Version 1: US Only) ===")
    current_df = spark.read.format("delta").load(delta_path)
    print(f"Version 1 Row Count: {current_df.count()}")
    current_df.show()

    # Step 3: TIME TRAVEL! Query historical Version 0 before the overwrite
    version_0_df = (
        spark.read
        .format("delta")
        .option("versionAsOf", 0)
        .load(delta_path)
    )

    print("=== Time-Traveled Table State (Version 0: All Users Restored) ===")
    print(f"Version 0 Historical Row Count: {version_0_df.count()}")
    version_0_df.show()

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson23: Lesson = {
  id: 123,
  title: 'Spark Lab 23 · Time Travel & Table History in Delta',
  concept: `Because Delta Lake tracks every commit in its transaction log without immediately overwriting underlying Parquet files, you can query **exact historical versions** of your data lake table!

### Real-World Use Cases for Time Travel:
1. **Auditing & Compliance (GDPR/SOX)**: Prove what a table looked like at an exact timestamp in the past.
2. **Reproducible Machine Learning**: Train models on exact dataset versions (\`versionAsOf=42\`) to ensure results can be replicated.
3. **Disaster Recovery**: Accidental \`DELETE\` or bad ETL job? Instantly rollback or query the pre-corrupted state without restoring from backup!

### How to Query Historical Snapshots:
\`\`\`python
# By commit version number:
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)

# By timestamp:
df_yesterday = spark.read.format("delta").option("timestampAsOf", "2024-03-01 00:00:00").load(path)
\`\`\`

In this lab, you will write multiple commits to a Delta table, overwrite records, and use \`versionAsOf\` time travel to inspect the original state.`,
  initialFiles: {
    'time_travel.py': TIME_TRAVEL_PY,
    'data/users.csv': USERS_CSV,
  },
  tasks: [
    {
      id: 'verify_time_travel_code',
      prompt: "Examine `time_travel.py` to confirm `versionAsOf` is configured when loading the historical snapshot.",
      hint: "Check that `.option('versionAsOf', 0)` is chained on the Delta read stream.",
      validate: (s) =>
        fileContains(s, 'time_travel.py', 'versionAsOf') &&
        fileContains(s, 'time_travel.py', 'format("delta")'),
    },
    {
      id: 'run_time_travel_job',
      prompt: 'Execute the time travel pipeline: `spark-submit time_travel.py`.',
      hint: 'Type `spark-submit time_travel.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('time_travel') ||
        Boolean(s.lastRun?.args?.includes('time_travel.py')),
    },
  ],
  quiz: {
    question: 'How does Delta Lake allow querying historical versions without requiring full table backups on every commit?',
    options: [
      'It stores data in browser cookies',
      'It uses copy-on-write; new files are added for changes while old Parquet files remain intact and referenced by historical log commits',
      'It reconstructs data by reversing network packets',
      'It runs a Git repo inside the JVM',
    ],
    correctIndex: 1,
    explanation: 'Delta Lake is an append-only system: modified data is written to new Parquet files, while older Parquet files remain on disk until vacuumed, allowing exact point-in-time reads.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'v0_commit', label: 'Delta Snapshot V0', layer: 'source' },
        { id: 'v1_commit', label: 'Delta Overwrite V1', layer: 'staging' },
        { id: 'timetravel_query', label: 'Time Travel (versionAsOf=0)', layer: 'mart' },
      ],
      edges: [
        { source: 'v0_commit', target: 'v1_commit' },
        { source: 'v0_commit', target: 'timetravel_query' },
      ],
    },
  },
  furtherReading: [
    { label: 'Delta Lake Time Travel Guide', url: 'https://docs.delta.io/latest/delta-utility.html#history' },
  ],
}

export default sparkLesson23
