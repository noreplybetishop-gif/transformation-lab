import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'

const JVM_GC_TUNING_PY = `"""
Lab 43: JVM Garbage Collection Tuning for Spark
Eliminate Stop-the-World pauses with G1GC, IHOP tuning, and region sizing.
"""

def generate_spark_gc_flags():
    """
    Standard Production JVM Garbage Collection flags for Spark Executors (30GB+ Heaps):
    - G1GC: Garbage-First collector designed for large multi-gigabyte heaps.
    - InitiatingHeapOccupancyPercent (IHOP): Triggers concurrent marking at 35% capacity before heap fills.
    - G1ReservePercent: Holds 15% memory in reserve as a safety buffer against evacuation failures.
    - MaxGCPauseMillis: Sets target pause goal to 200 milliseconds.
    """
    return [
        "-XX:+UseG1GC",
        "-XX:InitiatingHeapOccupancyPercent=35",
        "-XX:G1ReservePercent=15",
        "-XX:MaxGCPauseMillis=200",
        "-XX:G1HeapRegionSize=32m",
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:+G1SummarizeRSetStats"
    ]

def main():
    gc_flags = generate_spark_gc_flags()
    joined_flags = " ".join(gc_flags)

    spark_conf = {
        "spark.executor.extraJavaOptions": joined_flags,
        "spark.driver.extraJavaOptions": joined_flags,
    }

    print("=== PRODUCTION JVM GC FLAGS FOR SPARK ===")
    for flag in gc_flags:
        print(f"  • {flag}")
    print("=========================================")
    print(f"spark.executor.extraJavaOptions = \\"{spark_conf['spark.executor.extraJavaOptions']}\\"")
    print("✓ JVM GC parameters validated successfully.")

if __name__ == "__main__":
    main()
`

const sparkLesson43: Lesson = {
  id: 143,
  title: 'Spark Lab 43 · JVM Garbage Collection Tuning for Spark',
  concept: `Because Apache Spark executors run on the Java Virtual Machine (JVM), allocating and releasing millions of short-lived objects (such as row tuples, strings, and hash table buckets) puts tremendous pressure on JVM Garbage Collection.

### The Stop-The-World (STW) Catastrophe
Under the legacy ParallelGC collector, when the Old Generation heap fills up:
1. The JVM freezes all execution threads.
2. If the heap is 32 GB, the freeze can last **60 to 180 seconds**!
3. The cluster manager (YARN / Kubernetes) notices the executor stopped responding to heartbeats, assumes the worker node died, and kills the container.
4. Spark retries the failed stage, repeating the crash in an infinite loop!

### Tuning the G1GC Collector
The **G1 (Garbage-First)** collector is designed specifically for large heaps (8 GB+):
- It splits the heap into 2048 equal regions (e.g. 16MB or 32MB).
- It cleans regions with the highest amount of garbage first in small, incremental pauses.

### Crucial Production G1GC Flags

1. \`-XX:+UseG1GC\`: Enables Garbage-First collector.
2. \`-XX:InitiatingHeapOccupancyPercent=35\`: By default, G1GC waits until the heap is 45% full before starting background marking. Lowering this to **35%** starts garbage collection earlier, preventing full GC crashes during massive shuffle bursts.
3. \`-XX:G1ReservePercent=15\`: Reserves a 15% safety buffer so sudden bursts of object allocations don't run out of room before marking finishes.
4. \`-XX:MaxGCPauseMillis=200\`: Soft target instructing the JVM to keep pause times under 200 ms.`,
  initialFiles: {
    'jvm_gc_tuning.py': JVM_GC_TUNING_PY,
  },
  tasks: [
    {
      id: 'verify_gc_flags',
      prompt: "Ensure `jvm_gc_tuning.py` includes `-XX:+UseG1GC` and `-XX:InitiatingHeapOccupancyPercent=35`.",
      hint: "Check that UseG1GC and InitiatingHeapOccupancyPercent flags are present.",
      validate: (s) =>
        fileContains(s, 'jvm_gc_tuning.py', 'UseG1GC') &&
        fileContains(s, 'jvm_gc_tuning.py', 'InitiatingHeapOccupancyPercent=35'),
    },
    {
      id: 'run_gc_tuning_script',
      prompt: 'Run the GC tuning validator: `python jvm_gc_tuning.py`.',
      hint: 'Type `spark-submit jvm_gc_tuning.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('jvm_gc_tuning') ||
        Boolean(s.lastRun?.args?.includes('jvm_gc_tuning.py')),
    },
  ],
  quiz: {
    question: 'Why should InitiatingHeapOccupancyPercent (IHOP) be lowered from 45% to 35% in high-throughput Spark jobs?',
    options: [
      'To start background concurrent garbage marking earlier and prevent sudden heap exhaustion during shuffle spikes',
      'To force the JVM to shut down when memory reaches 35%',
      'To limit CPU usage to 35%',
      'To disable all garbage collection',
    ],
    correctIndex: 0,
    explanation: 'A lower IHOP initiates background concurrent marking cycles before the Old Generation fills up, avoiding catastrophic Stop-The-World evacuation failures.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'jvm', label: 'Large JVM Heap (32GB)', layer: 'source' },
        { id: 'g1gc', label: 'G1GC Incremental Regions (IHOP 35%)', layer: 'staging' },
        { id: 'smooth', label: 'Smooth < 200ms GC Pauses (Zero STW)', layer: 'mart' },
      ],
      edges: [
        { source: 'jvm', target: 'g1gc' },
        { source: 'g1gc', target: 'smooth' },
      ],
    },
  },
  furtherReading: [
    { label: 'Tuning Java Garbage Collection for Apache Spark', url: 'https://spark.apache.org/docs/latest/tuning.html#garbage-collection-tuning' },
  ],
}

export default sparkLesson43
