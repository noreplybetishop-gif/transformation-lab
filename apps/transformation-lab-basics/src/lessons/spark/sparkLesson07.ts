import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { USERS_CSV } from './_canonical'

const SPARK_SQL_PY = `"""
Lab 7: Spark SQL & Temp Views
Register DataFrames as temporary SQL views and query them using standard ANSI SQL.
"""

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("SparkSqlLab").getOrCreate()

    users_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv")

    # 1. Register temporary catalog view
    users_df.createOrReplaceTempView("v_users")

    # 2. Execute ANSI SQL query
    query = """
        SELECT
            country,
            COUNT(*) as total_users,
            ROUND(AVG(age), 1) as avg_age
        FROM v_users
        WHERE age >= 25
        GROUP BY country
        ORDER BY total_users DESC
    """
    result_df = spark.sql(query)

    print("=== Spark SQL Query Results ===")
    result_df.show()

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson07: Lesson = {
  id: 107,
  title: 'Spark Lab 7 · Spark SQL & Temp Views',
  concept: `If you already know SQL, you can use Spark's full distributed engine without learning proprietary APIs!

By calling:
\`\`\`python
df.createOrReplaceTempView("v_users")
\`\`\`
Spark registers the DataFrame as a temporary view in the in-memory catalog for the current \`SparkSession\`.

You can then run any ANSI SQL expression:
\`\`\`python
result_df = spark.sql("""
    SELECT country, COUNT(*) as users
    FROM v_users
    GROUP BY country
""")
result_df.show()
\`\`\`
The SQL query compiles into the **exact same Catalyst execution plan** as the DataFrame Python API! There is zero performance difference between writing PySpark code and writing SQL.`,
  initialFiles: {
    'spark_sql.py': SPARK_SQL_PY,
    'data/users.csv': USERS_CSV,
  },
  tasks: [
    {
      id: 'verify_temp_view',
      prompt: "Review `spark_sql.py` to inspect `createOrReplaceTempView('v_users')` and `spark.sql(...)`.",
      hint: "Check that createOrReplaceTempView is called on users_df.",
      validate: (s) =>
        fileContains(s, 'spark_sql.py', 'createOrReplaceTempView') &&
        fileContains(s, 'spark_sql.py', 'spark.sql'),
    },
    {
      id: 'run_spark_sql',
      prompt: 'Execute the Spark SQL application: `spark-submit spark_sql.py`.',
      hint: 'Run `spark-submit spark_sql.py` in the console.',
      validate: (s) =>
        s.ranModels.has('spark_sql') ||
        Boolean(s.lastRun?.args?.includes('spark_sql.py')),
    },
  ],
  quiz: {
    question: 'How does performance differ between writing transformations using the PySpark DataFrame API versus Spark SQL?',
    options: [
      'SQL is 10 times slower because it must be translated to Python',
      'Python is much faster because it skips the Java JVM',
      'They have identical performance: both compile into the same Catalyst optimized logical and physical execution plan',
      'SQL only works when connected to Postgres',
    ],
    correctIndex: 2,
    explanation: 'Both the Python DataFrame API and Spark SQL are translated into the Catalyst AST (Abstract Syntax Tree) and optimized identically by the Catalyst optimizer.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'csv_data', label: 'users.csv', layer: 'source' },
        { id: 'temp_view', label: 'v_users (Temp View)', layer: 'staging' },
        { id: 'sql_engine', label: 'spark.sql() (Catalyst Plan)', layer: 'mart' },
      ],
      edges: [
        { source: 'csv_data', target: 'temp_view' },
        { source: 'temp_view', target: 'sql_engine' },
      ],
    },
  },
  furtherReading: [
    { label: 'Spark SQL Getting Started', url: 'https://spark.apache.org/docs/latest/sql-getting-started.html' },
  ],
}

export default sparkLesson07
