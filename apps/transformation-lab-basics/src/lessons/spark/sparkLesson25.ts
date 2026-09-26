import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { WEB_LOGS_CSV } from './_canonical'

const BROADCAST_ACCUMULATOR_PY = `"""
Lab 25: Broadcast Variables & Accumulators
Share read-only lookup dictionaries and aggregate distributed counters across workers.
"""

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("BroadcastAccumulatorLab").getOrCreate()
    sc = spark.sparkContext

    # 1. Broadcast Variable: Send a read-only dictionary ONCE to every executor
    status_codes_lookup = {
        200: "OK_SUCCESS",
        404: "NOT_FOUND_ERROR",
        500: "INTERNAL_SERVER_ERROR"
    }
    broadcast_lookup = sc.broadcast(status_codes_lookup)

    # 2. Accumulator: A write-only distributed counter incremented by executors
    error_count = sc.accumulator(0)

    # Ingest logs
    logs_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/web_logs.csv")

    def process_partition(rows):
        lookup = broadcast_lookup.value
        results = []
        for row in rows:
            code = row["status_code"]
            desc = lookup.get(code, "UNKNOWN")
            if code >= 400:
                error_count.add(1)
            results.append((row["log_id"], code, desc, row["endpoint"]))
        return iter(results)

    # Execute partition mapping
    annotated_rdd = logs_df.rdd.mapPartitions(process_partition)
    sample_records = annotated_rdd.take(5)

    print("=== Annotated Logs with Broadcast Lookup ===")
    for record in sample_records:
        print(f"Log {record[0]}: HTTP {record[1]} ({record[2]}) -> {record[3]}")

    # Read accumulator value on the Driver
    print(f"Total HTTP 4xx/5xx Errors Detected Across Executors: {error_count.value}")

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson25: Lesson = {
  id: 125,
  title: 'Spark Lab 25 · Broadcast Variables & Accumulators',
  concept: `When distributed tasks execute on cluster worker nodes, standard Python variables are serialized and re-transmitted for **every individual task closure**, wasting network bandwidth and memory.

Spark provides two low-level distributed shared variable abstractions:

### 1. Broadcast Variables (\`sc.broadcast(val)\`)
- Sends a large read-only object (e.g. a 50MB machine learning lookup dictionary or geo-IP database) to each worker node **exactly once** using efficient peer-to-peer gossip transfer.
- Worker tasks read it locally via \`broadcast_var.value\` without shipping it over the network repeatedly.

### 2. Accumulators (\`sc.accumulator(initial_value)\`)
- A shared variable that executors can only **add to** (e.g. counting corrupted records, invalid timestamps, or tracking audit metrics).
- Workers cannot read the value (write-only for executors). Only the **Driver** can read the final summed \`accumulator.value\`.

In this lab, you will distribute an HTTP status lookup table using \`sc.broadcast\` and count anomalies using an \`accumulator\`.`,
  initialFiles: {
    'broadcast_accumulator.py': BROADCAST_ACCUMULATOR_PY,
    'data/web_logs.csv': WEB_LOGS_CSV,
  },
  tasks: [
    {
      id: 'verify_broadcast_accum_code',
      prompt: "Inspect `broadcast_accumulator.py` to confirm the use of `sc.broadcast` and `sc.accumulator`.",
      hint: "Check that `sc.broadcast(...)` and `sc.accumulator(...)` are instantiated.",
      validate: (s) =>
        fileContains(s, 'broadcast_accumulator.py', 'broadcast') &&
        fileContains(s, 'broadcast_accumulator.py', 'accumulator'),
    },
    {
      id: 'run_broadcast_accum_job',
      prompt: 'Execute the broadcast & accumulator pipeline: `spark-submit broadcast_accumulator.py`.',
      hint: 'Type `spark-submit broadcast_accumulator.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('broadcast_accumulator') ||
        Boolean(s.lastRun?.args?.includes('broadcast_accumulator.py')),
    },
  ],
  quiz: {
    question: 'Why can executor worker tasks NOT read the current value of an Accumulator during transformation execution?',
    options: [
      'It would cause Python syntax errors',
      'Reading in parallel tasks would require distributed coordination locks, violating lazy evaluation and hurting cluster throughput',
      'Accumulators are only stored on external USB drives',
      'Accumulators are erased after each row',
    ],
    correctIndex: 1,
    explanation: 'Accumulators are write-only for workers to ensure linear lock-free scalability. Only the Driver retrieves the final accumulated sum after job completion.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'driver_lookup', label: 'Driver sc.broadcast(lookup)', layer: 'source' },
        { id: 'executor_workers', label: 'Executor Workers (mapPartitions)', layer: 'staging' },
        { id: 'driver_counter', label: 'Driver accumulator.value', layer: 'mart' },
      ],
      edges: [
        { source: 'driver_lookup', target: 'executor_workers' },
        { source: 'executor_workers', target: 'driver_counter' },
      ],
    },
  },
  furtherReading: [
    { label: 'Spark Broadcast Variables & Accumulators Guide', url: 'https://spark.apache.org/docs/latest/rdd-programming-guide.html#shared-variables' },
  ],
}

export default sparkLesson25
