import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV } from './_canonical'

const MEMORY_MANAGEMENT_PY = `"""
Lab 33: Memory Management & Spill to Disk
Inspect Unified Memory Manager pools, storage fraction, and spill diagnostics.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder
        .appName("MemoryManagement")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.5")
        .config("spark.memory.offHeap.enabled", "false")
        .master("local[*]")
        .getOrCreate()
    )

    sc = spark.sparkContext
    conf = sc.getConf()

    print("=== SPARK UNIFIED MEMORY CONFIGURATIONS ===")
    mem_fraction = conf.get("spark.memory.fraction", "0.6")
    storage_fraction = conf.get("spark.memory.storageFraction", "0.5")
    off_heap = conf.get("spark.memory.offHeap.enabled", "false")

    print(f"Total Unified Spark Memory Fraction: {mem_fraction} (60% of Heap - 300MB)")
    print(f"Immune Storage Memory Fraction:      {storage_fraction} (50% of Spark Memory)")
    print(f"Off-Heap Memory Enabled:             {off_heap}")

    # Load orders and cache to demonstrate storage memory allocation
    orders = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")
    cached_orders = orders.cache()
    cached_count = cached_orders.count()

    print(f"Cached {cached_count} orders in Unified Storage Memory.")
    print("✓ Memory parameters configured and verified successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson33: Lesson = {
  id: 133,
  title: 'Spark Lab 33 · Memory Management & Spill to Disk',
  concept: `Every Spark JVM Executor allocates memory using the **Unified Memory Manager** introduced in Spark 1.6:

\`\`\`
┌────────────────────────────────────────────────────────────────────────┐
│ JVM Executor Heap Total (e.g. 16 GB)                                    │
│                                                                        │
│ ┌──────────────────────┐  ┌──────────────────────────────────────────┐ │
│ │ User Memory (40%)    │  │ Spark Memory Pool (60% by default)       │ │
│ │ User data structures,│  │  spark.memory.fraction = 0.6             │ │
│ │ UDF objects, Spark   │  │ ┌────────────────────┬─────────────────┐ │ │
│ │ metadata & hashes    │  │ │ Storage Pool (50%) │ Execution (50%) │ │ │
│ │                      │  │ │ Cached tables, RDD │ Shuffles, Joins,│ │ │
│ │                      │  │ │ broadcast variables│ Aggregations    │ │ │
│ └──────────────────────┘  │ └────────────────────┴─────────────────┘ │ │
│ ┌──────────────────────┐  └──────────────────────────────────────────┘ │
│ │ Reserved (300 MB)    │                                               │
│ └──────────────────────┘                                               │
└────────────────────────────────────────────────────────────────────────┘
\`\`\`

### Execution vs Storage Borrowing
- **Execution Memory** is used for shuffles, joins, sorts, and aggregations.
- **Storage Memory** is used for cached DataFrames (\`.cache()\`, \`.persist()\`) and broadcast variables.
- When no execution memory is needed, storage can borrow all of execution memory!
- But when execution needs memory, it can **evict cached storage blocks** to disk, but storage can **never evict execution memory**!

### What is Spill (Memory) and Spill (Disk)?
When execution memory fills up during a massive sort or aggregate:
- **Spill (Memory)**: The uncompressed size of records in RAM before writing.
- **Spill (Disk)**: The serialized and compressed size written to local NVMe/EBS disk.
Spill severely slows down stages because worker cores must perform serial disk I/O instead of in-memory transformations.`,
  initialFiles: {
    'memory_management.py': MEMORY_MANAGEMENT_PY,
    'data/orders.csv': ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_memory_fraction',
      prompt: "Ensure `memory_management.py` sets `spark.memory.fraction` and `spark.memory.storageFraction`.",
      hint: "Check config('spark.memory.fraction', '0.6') is present in the builder.",
      validate: (s) =>
        fileContains(s, 'memory_management.py', 'spark.memory.fraction') &&
        fileContains(s, 'memory_management.py', 'spark.memory.storageFraction'),
    },
    {
      id: 'run_memory_script',
      prompt: 'Run the memory diagnostics script: `spark-submit memory_management.py`.',
      hint: 'Type `spark-submit memory_management.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('memory_management') ||
        Boolean(s.lastRun?.args?.includes('memory_management.py')),
    },
  ],
  quiz: {
    question: 'If Spark runs out of execution memory during a sort-merge join, what happens?',
    options: [
      'It immediately cancels the entire cloud account',
      'It evicts cached storage blocks if possible, or spills intermediate records to disk',
      'It switches to single-threaded Python without notifying anyone',
      'It compresses the driver process into a zip archive',
    ],
    correctIndex: 1,
    explanation: 'Spark execution memory takes precedence: it evicts cached partitions from storage memory or spills intermediate execution buffers to worker local disk to avoid OutOfMemory errors.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'heap', label: 'Executor JVM Heap', layer: 'source' },
        { id: 'unified', label: 'Unified Memory Pool (60%)', layer: 'staging' },
        { id: 'exec_stor', label: 'Dynamic Borrowing (Exec vs Storage)', layer: 'mart' },
      ],
      edges: [
        { source: 'heap', target: 'unified' },
        { source: 'unified', target: 'exec_stor' },
      ],
    },
  },
  furtherReading: [
    { label: 'Apache Spark Memory Management Architecture', url: 'https://spark.apache.org/docs/latest/tuning.html#memory-management-overview' },
  ],
}

export default sparkLesson33
