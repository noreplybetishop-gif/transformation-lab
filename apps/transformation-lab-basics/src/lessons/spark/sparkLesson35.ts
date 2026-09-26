import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { WEB_LOGS_CSV } from './_canonical'

const SPARK_UI_DIAGNOSTICS_PY = `"""
Lab 35: Diagnosing the Spark UI & Event Logs
Capture runtime execution metrics, stage durations, and shuffle statistics.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

def main():
    spark = (
        SparkSession.builder
        .appName("SparkUIDiagnostics")
        .config("spark.eventLog.enabled", "false")
        .master("local[*]")
        .getOrCreate()
    )

    sc = spark.sparkContext
    status_tracker = sc.statusTracker()

    web_logs = spark.read.option("header", "true").option("inferSchema", "true").csv("data/web_logs.csv")

    # Run multi-stage workload
    metrics = (
        web_logs
        .groupBy("endpoint")
        .agg(
            count("log_id").alias("hit_count"),
            avg("response_ms").alias("avg_latency")
        )
        .orderBy(col("hit_count").desc())
    )

    metrics.show()

    # Query StatusTracker for Job and Stage diagnostic IDs
    job_ids = status_tracker.getJobIdsForGroup(None)
    active_stage_ids = status_tracker.getActiveStageIds()

    print("=== SPARK UI RUNTIME DIAGNOSTIC METRICS ===")
    print(f"Total Spark Jobs Executed: {len(job_ids)}")
    print(f"Active Stages in Scheduler: {len(active_stage_ids)}")
    print(f"Default Parallelism:       {sc.defaultParallelism}")
    print("✓ Diagnostic metrics captured successfully.")

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson35: Lesson = {
  id: 135,
  title: 'Spark Lab 35 · Diagnosing the Spark UI & Event Logs',
  concept: `The **Spark Web UI** (defaulting to \`http://<driver-node>:4040\`) is the indispensable tool for debugging and optimizing distributed Spark applications.

### Key Spark UI Tabs and What to Look For

1. **Jobs Tab**:
   - Lists actions triggered in order. Look at the **Event Timeline** to spot long initialization delays or idle executor times.

2. **Stages Tab**:
   - Each stage represents a pipelined sequence of transformations up to a **Shuffle Boundary**.
   - Look at the **Summary Metrics table**:
     - **Task Deserialization Time vs Executor Run Time**: If deserialization dominates, your tasks are too small!
     - **Shuffle Read / Write Size**: Check for uneven skew across the 75th percentile and Maximum task.
     - **GC Time**: If GC time exceeds 10% of total task duration, the JVM is struggling with object thrashing.
     - **Spill (Memory) / Spill (Disk)**: Warns that partitions did not fit into execution memory.

3. **Storage Tab**:
   - Shows DataFrames persisted in memory (\`.cache()\`), fraction cached, and memory vs disk footprints.

4. **SQL / DataFrame Tab**:
   - Visual DAG of the Catalyst query plan, showing physical node statistics like \`number of output rows\`, \`data size\`, and codegen IDs.`,
  initialFiles: {
    'spark_ui_diagnostics.py': SPARK_UI_DIAGNOSTICS_PY,
    'data/web_logs.csv': WEB_LOGS_CSV,
  },
  tasks: [
    {
      id: 'verify_tracker_usage',
      prompt: "Ensure `spark_ui_diagnostics.py` queries `status_tracker` to inspect executed jobs and stages.",
      hint: "Check that sc.statusTracker() is utilized in the script.",
      validate: (s) =>
        fileContains(s, 'spark_ui_diagnostics.py', 'statusTracker') &&
        fileContains(s, 'spark_ui_diagnostics.py', 'defaultParallelism'),
    },
    {
      id: 'run_diagnostics_script',
      prompt: 'Execute the diagnostics script: `spark-submit spark_ui_diagnostics.py`.',
      hint: 'Type `spark-submit spark_ui_diagnostics.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('spark_ui_diagnostics') ||
        Boolean(s.lastRun?.args?.includes('spark_ui_diagnostics.py')),
    },
  ],
  quiz: {
    question: 'In the Spark UI Stages tab, what metric clearly indicates that a stage suffered from data skew?',
    options: [
      'The median task time and 75th percentile task time are 2 seconds, but the Max task time is 45 minutes',
      'The Spark logo in the top left turns purple',
      'The cluster has zero jobs queued',
      'The driver runs on port 4040',
    ],
    correctIndex: 0,
    explanation: 'A huge gap between the 75th percentile task duration and the maximum task duration is the signature indicator of partition data skew: one executor received far more data than the rest.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'logs', label: 'web_logs.csv', layer: 'source' },
        { id: 'ui', label: 'SparkUI Event Tracker', layer: 'staging' },
        { id: 'metrics', label: 'Stage & Job Diagnostics', layer: 'mart' },
      ],
      edges: [
        { source: 'logs', target: 'ui' },
        { source: 'ui', target: 'metrics' },
      ],
    },
  },
  furtherReading: [
    { label: 'Monitoring Spark Applications Using the Web UI', url: 'https://spark.apache.org/docs/latest/web-ui.html' },
  ],
}

export default sparkLesson35
