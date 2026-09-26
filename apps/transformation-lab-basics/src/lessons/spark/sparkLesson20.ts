import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { USERS_CSV } from './_canonical'

const UDF_BASICS_PY = `"""
Lab 20: User-Defined Functions (UDFs) & Pandas UDFs
Extend Spark with custom Python transformations and vectorized Pandas UDFs.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType

# 1. Define standard Python function
def mask_name(name):
    if not name:
        return ""
    parts = name.split(" ")
    if len(parts) > 1:
        return f"{parts[0]} {parts[1][0]}."
    return parts[0]

# 2. Register Python function as a Spark UDF with explicit return type
mask_name_udf = udf(mask_name, StringType())

def main():
    spark = SparkSession.builder.appName("UdfLab").getOrCreate()

    users_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv")

    # 3. Apply UDF to DataFrame column
    anonymized_df = (
        users_df
        .withColumn("masked_name", mask_name_udf(col("name")))
        .select("id", "name", "masked_name", "country")
    )

    print("=== Custom UDF Output ===")
    anonymized_df.show(5)

    # 4. In production: Vectorized Pandas UDFs (Arrow-based) execute in batches
    # @pandas_udf(DoubleType())
    # def vectorized_calc(series: pd.Series) -> pd.Series:
    #     return series * 1.05

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson20: Lesson = {
  id: 120,
  title: 'Spark Lab 20 · User-Defined Functions (UDFs) & Pandas UDFs',
  concept: `When built-in Spark SQL functions cannot express your business logic, you can register **User-Defined Functions (UDFs)**.

### The Problem with Standard Python UDFs:
1. **JVM <-> Python Serialization**: Spark's core engine runs on the JVM. A Python UDF requires serializing row data from the JVM, transferring it over a socket to a Python worker process, running the function row-by-row, and serializing the result back to the JVM!
2. **Loss of Catalyst Optimization**: Spark treats standard Python UDFs as a "black box" and cannot push down filters or vectorize operations.

### Vectorized Pandas UDFs (\`@pandas_udf\`)
Introduced in Spark 2.3, **Pandas UDFs** use **Apache Arrow** to transfer data in zero-copy columnar batches rather than row-by-row.
\`\`\`python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def add_sales_tax(amounts: pd.Series) -> pd.Series:
    return amounts * 1.0825  # Vectorized C-speed SIMD math!
\`\`\`
- Often **10x to 100x faster** than standard Python UDFs!

### Best Practice Hierarchy:
1. Always prefer native Spark functions (\`pyspark.sql.functions\`).
2. If custom logic is required, use Vectorized Pandas UDFs.
3. Use standard Python UDFs only as a last resort.

In this lab, you will register a custom UDF, specify its return type, and transform DataFrame columns.`,
  initialFiles: {
    'udf_basics.py': UDF_BASICS_PY,
    'data/users.csv': USERS_CSV,
  },
  tasks: [
    {
      id: 'verify_udf_code',
      prompt: "Inspect `udf_basics.py` to confirm the definition of `mask_name` and registration with `udf()`.",
      hint: "Check that `udf(mask_name, StringType())` is assigned to a variable.",
      validate: (s) =>
        fileContains(s, 'udf_basics.py', 'def mask_name') &&
        fileContains(s, 'udf_basics.py', 'udf('),
    },
    {
      id: 'run_udf_job',
      prompt: 'Execute your custom UDF script: `spark-submit udf_basics.py`.',
      hint: 'Type `spark-submit udf_basics.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('udf_basics') ||
        Boolean(s.lastRun?.args?.includes('udf_basics.py')),
    },
  ],
  quiz: {
    question: 'Why are native PySpark functions (like col, when, concat) significantly faster than custom Python UDFs?',
    options: [
      'They delete null rows automatically',
      'They execute directly in native JVM bytecode via Project Tungsten without serializing data between Python and the JVM',
      'They only work in cloud environments',
      'They don\'t use RAM',
    ],
    correctIndex: 1,
    explanation: 'Native Spark SQL functions execute directly inside the Tungsten execution engine on the JVM without IPC socket communication or data deserialization into Python objects.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'jvm_data', label: 'JVM DataFrame Data', layer: 'source' },
        { id: 'python_worker', label: 'Python Worker (mask_name)', layer: 'staging' },
        { id: 'output_col', label: 'Masked Column Output', layer: 'mart' },
      ],
      edges: [
        { source: 'jvm_data', target: 'python_worker' },
        { source: 'python_worker', target: 'output_col' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark User Defined Functions (UDFs)', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html' },
    { label: 'Vectorized Pandas UDFs in Apache Spark', url: 'https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html' },
  ],
}

export default sparkLesson20
