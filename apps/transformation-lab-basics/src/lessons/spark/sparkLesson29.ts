import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { ORDERS_CSV } from './_canonical'

const TUNGSTEN_CODEGEN_PY = `"""
Lab 29: Whole-Stage Code Generation (Tungsten)
Examine Project Tungsten bytecode compilation and CPU register caching.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

def main():
    spark = (
        SparkSession.builder
        .appName("TungstenCodegen")
        .config("spark.sql.codegen.wholeStage", "true")
        .master("local[*]")
        .getOrCreate()
    )

    orders = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")

    # Pipeline with filter and calculation
    result = (
        orders
        .filter(col("status") == "COMPLETED")
        .select(
            col("order_id"),
            round(col("amount") * 1.08, 2).alias("tax_inclusive_amount")
        )
    )

    print("=== TUNGSTEN PHYSICAL EXECUTION PLAN ===")
    # Look for *(1) markers indicating WholeStageCodegen operators
    result.explain(mode="formatted")

    print("✓ Tungsten WholeStageCodegen pipeline evaluated.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson29: Lesson = {
  id: 129,
  title: 'Spark Lab 29 · Whole-Stage Code Generation (Tungsten)',
  concept: `**Project Tungsten** revolutionized Spark engine performance by shifting the execution bottleneck from I/O to CPU and memory efficiency.

### What is Whole-Stage Code Generation?
In classic database engines (like Volcano iterator models), every tuple processed involves calling \`next()\` virtual methods on operator classes. For millions of rows, this produces:
1. Virtual function dispatch overhead
2. Data thrashing across L1/L2/L3 CPU caches
3. JVM boxing/unboxing object allocation penalties

**Whole-Stage Code Generation (WSCG)** compiles an entire pipeline of physical transformations (like Filter -> Project -> Aggregate) into a single, compact Java \`for\`-loop bytecode. The data remains directly in CPU registers!

### Recognizing WSCG in Plans
When inspecting a physical execution plan with \`df.explain()\`, notice asterisks before operators:
\`\`\`
*(1) Project [order_id#0, round((amount#2 * 1.08), 2) AS tax_inclusive_amount#10]
+- *(1) Filter (isnotnull(status#3) AND (status#3 = COMPLETED))
   +- FileScan csv [order_id#0,amount#2,status#3]
\`\`\`
The \`*(1)\` denotes that the FileScan reading, the Filter, and the Project are all compiled together into **Stage 1 compiled Java bytecode**!`,
  initialFiles: {
    'tungsten_codegen.py': TUNGSTEN_CODEGEN_PY,
    'data/orders.csv': ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_tungsten_config',
      prompt: "Ensure `tungsten_codegen.py` sets `spark.sql.codegen.wholeStage` to `'true'` in the builder configuration.",
      hint: "Check config('spark.sql.codegen.wholeStage', 'true') is present in the builder.",
      validate: (s) => fileContains(s, 'tungsten_codegen.py', 'spark.sql.codegen.wholeStage'),
    },
    {
      id: 'run_tungsten_script',
      prompt: 'Execute the Tungsten codegen inspector: `spark-submit tungsten_codegen.py`.',
      hint: 'Type `spark-submit tungsten_codegen.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('tungsten_codegen') ||
        Boolean(s.lastRun?.args?.includes('tungsten_codegen.py')),
    },
  ],
  quiz: {
    question: 'In a Spark physical execution plan, what does the asterisk `*(1)` preceding an operator indicate?',
    options: [
      'The operator is executing on an external GPU',
      'The operator is fused into Whole-Stage Code Generation stage 1',
      'The operator failed and is falling back to Python',
      'The operator requires a network shuffle boundary',
    ],
    correctIndex: 1,
    explanation: 'The asterisk symbol `*(n)` signifies that the operator is part of whole-stage codegen sub-tree number n, compiled into a single optimized Java function.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'csv', label: 'FileScan csv', layer: 'source' },
        { id: 'wscg', label: '*(1) WholeStageCodegen Loop', layer: 'staging' },
        { id: 'cpu', label: 'CPU Register Execution', layer: 'mart' },
      ],
      edges: [
        { source: 'csv', target: 'wscg' },
        { source: 'wscg', target: 'cpu' },
      ],
    },
  },
  furtherReading: [
    { label: 'Project Tungsten: Bringing Spark Closer to Bare Metal', url: 'https://www.databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html' },
  ],
}

export default sparkLesson29
