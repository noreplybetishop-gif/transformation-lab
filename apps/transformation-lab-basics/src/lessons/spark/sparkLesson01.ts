import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'

const SPARK_SESSION_PY = `"""
Lab 1: The Spark Driver & SparkSession
Initialize a SparkSession and inspect the cluster environment.
"""

from pyspark.sql import SparkSession

def main():
    # 1. Create a SparkSession with appName "BasicsLab1"
    spark = (
        SparkSession.builder
        .appName("BasicsLab1")
        .master("local[*]")
        .getOrCreate()
    )

    print("=== SparkSession Initialized ===")
    print(f"Application Name: {spark.sparkContext.appName}")
    print(f"Spark Version: {spark.version}")
    print(f"Master: {spark.sparkContext.master}")

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson01: Lesson = {
  id: 101,
  title: 'Spark Lab 1 · The Spark Driver & SparkSession',
  concept: `Welcome to **Apache Spark Lab**!

In Apache Spark, every application begins with the **Driver Process**. The driver maintains cluster state, coordinates job execution, and translates your code into a physical execution DAG.

Since Spark 2.0, the unified entry point for all DataFrame and SQL functionality is the **\`SparkSession\`**:
\`\`\`python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MySparkApp")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
\`\`\`
- **\`appName\`**: Identifies your job in the Spark Web UI (port 4040) and cluster manager.
- **\`master\`**: Specifies cluster connection string (e.g. \`local[*]\` for all CPU cores, or \`yarn\` / \`k8s://...\`).
- **\`getOrCreate()\`**: Retrieves an existing session or instantiates a new singleton.

In this lab, you will configure a \`SparkSession\` and execute your first Spark job!`,
  initialFiles: {
    'spark_session.py': SPARK_SESSION_PY,
  },
  tasks: [
    {
      id: 'inspect_session',
      prompt: "Review `spark_session.py` to ensure `SparkSession.builder` and `appName('BasicsLab1')` are configured.",
      hint: "Check that appName('BasicsLab1') is set on the builder.",
      validate: (s) =>
        fileContains(s, 'spark_session.py', 'SparkSession') &&
        fileContains(s, 'spark_session.py', 'BasicsLab1'),
    },
    {
      id: 'run_job',
      prompt: 'Execute your Spark application: `spark-submit spark_session.py`.',
      hint: 'Type `spark-submit spark_session.py` in the terminal and press Enter.',
      validate: (s) =>
        s.ranModels.has('spark_session') ||
        Boolean(s.lastRun?.args?.includes('spark_session.py')),
    },
  ],
  quiz: {
    question: 'What is the primary role of the Spark Driver in a distributed cluster?',
    options: [
      'It stores raw files permanently on hard drives',
      'It maintains application state, runs main(), schedules tasks, and coordinates worker executors',
      'It compiles SQL directly into JavaScript',
      'It encrypts internet network traffic',
    ],
    correctIndex: 1,
    explanation: 'The Spark Driver process coordinates the entire application lifecycle, maintaining the SparkSession, scheduling DAG tasks, and assigning partitions to worker executors.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'driver', label: 'Spark Driver (main)', layer: 'source' },
        { id: 'session', label: 'SparkSession', layer: 'staging' },
        { id: 'context', label: 'SparkContext (local[*])', layer: 'mart' },
      ],
      edges: [
        { source: 'driver', target: 'session' },
        { source: 'session', target: 'context' },
      ],
    },
  },
  furtherReading: [
    { label: 'Starting Point: SparkSession', url: 'https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession' },
  ],
}

export default sparkLesson01
