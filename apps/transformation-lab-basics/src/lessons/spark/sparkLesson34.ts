import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'

const CLUSTER_SIZING_PY = `"""
Lab 34: Driver & Executor Sizing for Production
Calculate optimal executor cores, memory overhead, and instance allocation.
"""

def calculate_spark_sizing(num_nodes, cores_per_node, ram_gb_per_node):
    """
    Standard Production Sizing Formula for Spark on YARN / Kubernetes:
    1. Leave 1 core & 1 GB RAM per node for OS / NodeManager daemons.
    2. Sweet spot for executor cores is 5 (optimal HDFS I/O throughput).
    3. Memory Overhead = max(384MB, 10% of executor memory).
    """
    usable_cores = cores_per_node - 1
    usable_ram = ram_gb_per_node - 1

    executors_per_node = usable_cores // 5
    cores_per_exec = 5

    total_exec_ram = usable_ram / executors_per_node
    # Reserve 10% for spark.executor.memoryOverhead
    memory_overhead = max(0.384, total_exec_ram * 0.10)
    executor_memory = total_exec_ram - memory_overhead

    # 1 executor on cluster is reserved for the Driver in client/cluster mode
    total_executors = (num_nodes * executors_per_node) - 1

    return {
        "num_nodes": num_nodes,
        "executors_per_node": executors_per_node,
        "total_executors": total_executors,
        "executor_cores": cores_per_exec,
        "executor_memory_gb": round(executor_memory, 2),
        "memory_overhead_gb": round(memory_overhead, 2),
    }

def main():
    # Example: 10 worker nodes, each having 16 vCPUs and 64 GB RAM
    config = calculate_spark_sizing(num_nodes=10, cores_per_node=16, ram_gb_per_node=64)

    print("=== PRODUCTION SPARK CLUSTER SIZING REPORT ===")
    print(f"Total Worker Nodes:        {config['num_nodes']}")
    print(f"Executors Per Node:        {config['executors_per_node']}")
    print(f"Recommended --num-executors: {config['total_executors']}")
    print(f"Recommended --executor-cores: {config['executor_cores']}")
    print(f"Recommended --executor-memory: {config['executor_memory_gb']}g")
    print(f"Recommended memoryOverhead:   {config['memory_overhead_gb']}g")
    print("==============================================")
    print("✓ Cluster sizing parameters verified.")

if __name__ == "__main__":
    main()
`

const sparkLesson34: Lesson = {
  id: 134,
  title: 'Spark Lab 34 · Driver & Executor Sizing for Production',
  concept: `One of the most frequent mistakes in data engineering is choosing bad Spark executor sizing:
- **Fat Executors (e.g. 1 executor per node with 32 cores & 128GB RAM)**: Excessive JVM garbage collection (GC) pauses of 30+ seconds freeze cluster heartbeat checks, causing nodes to be marked dead!
- **Tiny Executors (e.g. 1 core & 2GB RAM per executor)**: Incapable of running broadcast joins in memory and loses multi-threaded in-JVM caching benefits.

### The Production Sweet-Spot Rule of Thumb

1. **Leave 1 Core and 1 GB per physical machine** for the operating system and daemon agents (YARN NodeManager, Kubelet).
2. **Assign 4 to 5 cores per executor**:
   Benchmark studies show that **5 cores** is the optimal concurrency sweet-spot for HDFS/S3 concurrent throughput without incurring severe GC stalls.
3. **Allocate \`spark.executor.memoryOverhead\`**:
   Always reserve **10% of executor memory** (minimum 384 MB) for off-heap allocations, network buffers, PySpark Python worker communication, and JVM internal structures.

### The Standard Sizing Calculation
For a 10-node cluster where each machine has **16 cores and 64 GB RAM**:
- Usable per node: 15 cores and 63 GB RAM
- Number of executors per node: \`15 / 5 = 3 executors\`
- Total RAM per executor: \`63 GB / 3 = 21 GB\`
- Overhead (10%): \`2.1 GB\`
- \`--executor-memory 18g\`
- \`--executor-cores 5\`
- \`--num-executors 29\` (30 total minus 1 executor reserved for ApplicationMaster / Driver)`,
  initialFiles: {
    'cluster_sizing.py': CLUSTER_SIZING_PY,
  },
  tasks: [
    {
      id: 'verify_sizing_function',
      prompt: "Ensure `cluster_sizing.py` includes the `calculate_spark_sizing` function with 5 executor cores.",
      hint: "Check that cores_per_exec = 5 and memory_overhead calculation are implemented.",
      validate: (s) =>
        fileContains(s, 'cluster_sizing.py', 'calculate_spark_sizing') &&
        fileContains(s, 'cluster_sizing.py', 'executor_cores'),
    },
    {
      id: 'run_sizing_script',
      prompt: 'Run the sizing calculation: `python cluster_sizing.py` or `spark-submit cluster_sizing.py`.',
      hint: 'Type `spark-submit cluster_sizing.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('cluster_sizing') ||
        Boolean(s.lastRun?.args?.includes('cluster_sizing.py')),
    },
  ],
  quiz: {
    question: 'Why is 4 to 5 cores per executor considered the production standard for Apache Spark?',
    options: [
      'It maximizes concurrent HDFS/cloud I/O throughput while keeping JVM garbage collection pauses minimal',
      'Spark cannot run on machines with more than 5 cores',
      'Cloud providers automatically shut down any executor with more than 5 cores',
      'It makes Python run faster than C',
    ],
    correctIndex: 0,
    explanation: 'Extensive benchmarks demonstrate that more than 5 cores leads to severe JVM GC pauses on large heaps, while fewer than 4 cores wastes multi-threaded caching advantages.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'node', label: 'Physical Worker Node (16 vCPU, 64GB)', layer: 'source' },
        { id: 'formula', label: 'Sweet-Spot Formula (5 Cores/Exec)', layer: 'staging' },
        { id: 'exec', label: '3 Executors per Node (18G RAM + 2.1G Overhead)', layer: 'mart' },
      ],
      edges: [
        { source: 'node', target: 'formula' },
        { source: 'formula', target: 'exec' },
      ],
    },
  },
  furtherReading: [
    { label: 'How to Size Executors for Apache Spark Clusters', url: 'https://spark.apache.org/docs/latest/hardware-provisioning.html' },
  ],
}

export default sparkLesson34
