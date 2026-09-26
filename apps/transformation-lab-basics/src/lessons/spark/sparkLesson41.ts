import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'

const CLOUD_COST_OPTIMIZATION_PY = `"""
Lab 41: Cost Optimization on Cloud Spark (EMR / Databricks)
Architect cloud infrastructure with Spot instances, Graviton ARM, and auto-scaling.
"""

def generate_cloud_cluster_spec():
    """
    Enterprise Cloud Spark Cost Optimization Template:
    1. Driver / Master Node: On-Demand instance (guarantees job stability).
    2. Core / Primary Workers (20%): On-Demand with HDFS/Delta metadata.
    3. Task / Secondary Workers (80%): Spot / Preemptible instances (60-80% discount).
    4. Decommissioning: spark.decommission.enabled=true for graceful Spot loss.
    5. Architecture: ARM Graviton (c6g / m6g) for 20% lower cost/core vs x86.
    """
    return {
        "cluster_name": "prod-analytics-lakehouse-etl",
        "cloud_provider": "AWS_EMR_OR_DATABRICKS",
        "instance_family": "ARM64_GRAVITON3",
        "driver_pool": {
            "instance_type": "m6g.2xlarge",
            "market": "ON_DEMAND",
            "count": 1
        },
        "core_node_pool": {
            "instance_type": "r6g.4xlarge",
            "market": "ON_DEMAND",
            "percentage": 20,
            "min_nodes": 2,
            "max_nodes": 6
        },
        "task_node_pool": {
            "instance_type": "r6g.4xlarge",
            "market": "SPOT",
            "percentage": 80,
            "min_nodes": 4,
            "max_nodes": 32,
            "savings_vs_ondemand": "72%"
        },
        "spark_flags": [
            ("spark.decommission.enabled", "true"),
            ("spark.storage.decommission.enabled", "true"),
            ("spark.dynamicAllocation.enabled", "true"),
            ("spark.dynamicAllocation.idleTimeout", "60s")
        ]
    }

def main():
    spec = generate_cloud_cluster_spec()

    print("=== CLOUD SPARK FINOPS & ARCHITECTURE CONFIGURATION ===")
    print(f"Cluster:         {spec['cluster_name']}")
    print(f"Architecture:    {spec['instance_family']} (20% Price/Perf Gain)")
    print(f"Driver Node:     {spec['driver_pool']['instance_type']} ({spec['driver_pool']['market']})")
    print(f"Spot Node Fleet: {spec['task_node_pool']['percentage']}% of workers on Spot (Est. {spec['task_node_pool']['savings_vs_ondemand']} savings)")
    print("Crucial Resiliency Flags:")
    for key, val in spec['spark_flags']:
        print(f"  • {key} = {val}")

    print("✓ Cloud cost optimization blueprint validated.")

if __name__ == "__main__":
    main()
`

const sparkLesson41: Lesson = {
  id: 141,
  title: 'Spark Lab 41 · Cost Optimization on Cloud Spark (EMR / Databricks)',
  concept: `Cloud infrastructure costs can skyrocket rapidly without deliberate architecture. A staff data engineer must design clusters for optimal price-to-performance (FinOps).

### The 4 Pillars of Spark Cloud Cost Optimization

1. **Spot / Preemptible Instance Fleet (70%+ Savings)**:
   - **Driver & Master Node**: Always run on **On-Demand** instances! If the driver is killed by a Spot eviction, the entire job terminates immediately.
   - **Task Worker Nodes**: Run 70% to 90% of worker capacity on **Spot instances**.
   - **Graceful Decommissioning**:
     \`\`\`properties
     spark.decommission.enabled=true
     spark.storage.decommission.enabled=true
     \`\`\`
     When AWS or GCP issues a 2-minute termination notice, Spark migrates intermediate shuffle files and active tasks to surviving nodes before the VM is turned off.

2. **ARM / Graviton Processors (20% Price/Perf Improvement)**:
   Migrating from Intel Xeon (c5/m5) to ARM64 Graviton (c6g/r6g) delivers 15-20% lower hourly instance cost with identical or better JVM execution speeds.

3. **Dynamic Allocation & Aggressive Idle Timeouts**:
   \`\`\`properties
   spark.dynamicAllocation.enabled=true
   spark.dynamicAllocation.executorIdleTimeout=60s
   \`\`\`
   Do not pay for 50 idle nodes while a pipeline is waiting for downstream database connections.

4. **Right-Sizing Storage IOPS**:
   Use NVMe ephemeral local instance storage for shuffle data instead of expensive provisioned cloud SSDs (like gp3 or io2) which charge per IOPS.`,
  initialFiles: {
    'cloud_cost_optimization.py': CLOUD_COST_OPTIMIZATION_PY,
  },
  tasks: [
    {
      id: 'verify_cost_spec',
      prompt: "Ensure `cloud_cost_optimization.py` defines the cluster spec with `spark.decommission.enabled` and Spot node pools.",
      hint: "Check spark.decommission.enabled and task_node_pool SPOT settings in the script.",
      validate: (s) =>
        fileContains(s, 'cloud_cost_optimization.py', 'spark.decommission.enabled') &&
        fileContains(s, 'cloud_cost_optimization.py', 'SPOT'),
    },
    {
      id: 'run_cost_spec_script',
      prompt: 'Run the FinOps architecture validator: `python cloud_cost_optimization.py`.',
      hint: 'Type `spark-submit cloud_cost_optimization.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('cloud_cost_optimization') ||
        Boolean(s.lastRun?.args?.includes('cloud_cost_optimization.py')),
    },
  ],
  quiz: {
    question: 'Why should the Spark Driver process never be hosted on a cloud Spot / Preemptible instance in production?',
    options: [
      'If the driver node is reclaimed by the cloud provider, the entire application fails and all work is lost',
      'The driver only supports spinning hard disk drives',
      'Spot instances do not support IP addresses',
      'Java bytecode does not run on Spot instances',
    ],
    correctIndex: 0,
    explanation: 'The driver coordinates the entire SparkSession, DAGScheduler, and task dispatching. If the driver machine is evicted by a Spot reclamation event, the whole application crashes.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'driver', label: 'Driver Node (On-Demand)', layer: 'source' },
        { id: 'fleet', label: 'Worker Fleet (80% Spot + Graceful Decommission)', layer: 'staging' },
        { id: 'savings', label: '70% Cloud FinOps Cost Reduction', layer: 'mart' },
      ],
      edges: [
        { source: 'driver', target: 'fleet' },
        { source: 'fleet', target: 'savings' },
      ],
    },
  },
  furtherReading: [
    { label: 'Cost Optimization for Apache Spark on AWS EMR and Databricks', url: 'https://spark.apache.org/docs/latest/configuration.html#decommissioning' },
  ],
}

export default sparkLesson41
