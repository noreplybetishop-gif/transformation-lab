import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { DIRTY_USERS_CSV } from './_canonical'

const DATA_CLEANING_PY = `"""
Lab 10: Handling Nulls & Data Cleaning
Clean missing and corrupt values using dropna, fillna, and coalesce.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit

def main():
    spark = SparkSession.builder.appName("DataCleaningLab").getOrCreate()

    dirty_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/dirty_users.csv")

    print("=== Raw Dirty Data ===")
    dirty_df.show()

    # 1. Drop rows where primary key 'id' is null
    clean_ids = dirty_df.dropna(subset=["id"])

    # 2. Fill missing email with fallback placeholder
    filled_email = clean_ids.fillna({"email": "no-reply@company.com"})

    # 3. Use coalesce to substitute default country
    cleaned_df = filled_email.withColumn(
        "country",
        coalesce(col("country"), lit("GLOBAL"))
    )

    print("=== Cleaned Users DataFrame ===")
    cleaned_df.show()

    print(f"Original Count: {dirty_df.count()}, Cleaned Count: {cleaned_df.count()}")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson10: Lesson = {
  id: 110,
  title: 'Spark Lab 10 · Handling Nulls & Data Cleaning',
  concept: `Real-world data ingestion arrives with missing values, blank strings, and null pointers. Spark provides dedicated methods to sanitize DataFrames:

### 1. Dropping Nulls with \`dropna()\`
\`\`\`python
# Drop any row where 'id' or 'email' is null
clean_df = df.dropna(subset=["id", "email"])
\`\`\`

### 2. Imputing Values with \`fillna()\`
\`\`\`python
# Fill column-specific default values
filled_df = df.fillna({
    "email": "unknown@example.com",
    "country": "UNKNOWN",
    "score": 0.0
})
\`\`\`

### 3. Priority Fallbacks with \`coalesce()\`
\`\`\`python
# Evaluates arguments in order and returns the first non-null column/literal
from pyspark.sql.functions import coalesce, col, lit

df2 = df.withColumn("contact_email", coalesce(col("work_email"), col("personal_email"), lit("none")))
\`\`\``,
  initialFiles: {
    'data_cleaning.py': DATA_CLEANING_PY,
    'data/dirty_users.csv': DIRTY_USERS_CSV,
  },
  tasks: [
    {
      id: 'verify_cleaning',
      prompt: "Review `data_cleaning.py` to inspect `dropna(subset=['id'])`, `fillna()`, and `coalesce()`.",
      hint: "Check that dropna, fillna, and coalesce are used in data_cleaning.py.",
      validate: (s) =>
        fileContains(s, 'data_cleaning.py', 'dropna') &&
        fileContains(s, 'data_cleaning.py', 'fillna'),
    },
    {
      id: 'run_cleaning',
      prompt: 'Execute the data cleaning script: `spark-submit data_cleaning.py`.',
      hint: 'Run `spark-submit data_cleaning.py` in the console.',
      validate: (s) =>
        s.ranModels.has('data_cleaning') ||
        Boolean(s.lastRun?.args?.includes('data_cleaning.py')),
    },
  ],
  quiz: {
    question: 'What does df.dropna(subset=["user_id"]) do in PySpark?',
    options: [
      'Deletes the user_id column from the schema',
      'Removes only rows where the user_id column is null, keeping rows with nulls in other columns',
      'Replaces null user_ids with zero',
      'Throws an exception if any null exists',
    ],
    correctIndex: 1,
    explanation: 'The subset parameter restricts dropna to only check the specified column(s), ignoring null values that may exist elsewhere in the record.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'dirty_csv', label: 'dirty_users.csv', layer: 'source' },
        { id: 'dropna_step', label: 'dropna(subset=["id"])', layer: 'staging' },
        { id: 'fillna_step', label: 'fillna + coalesce', layer: 'mart' },
      ],
      edges: [
        { source: 'dirty_csv', target: 'dropna_step' },
        { source: 'dropna_step', target: 'fillna_step' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark DataFrameNaFunctions', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html' },
  ],
}

export default sparkLesson10
