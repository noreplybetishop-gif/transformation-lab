import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { USERS_CSV, ORDERS_CSV } from './_canonical'

const BATCH_PIPELINE_PY = `"""
Lab 12: Basics Capstone: Batch Aggregator
Orchestrate an end-to-end batch ingestion, cleaning, join, and aggregation pipeline.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, round, avg, coalesce, lit

def main():
    spark = SparkSession.builder.appName("SparkBasicsCapstone").getOrCreate()

    # Step 1: Ingest Users and Orders
    users_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv")
    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")

    # Step 2: Clean and filter completed orders
    clean_orders = orders_df.dropna(subset=["order_id", "user_id"]).filter(col("status") == "COMPLETED")

    # Step 3: Join datasets
    joined_df = clean_orders.join(users_df, on="user_id", how="inner")

    # Step 4: Aggregate revenue by country
    country_summary = (
        joined_df
        .groupBy("country")
        .agg(
            count("order_id").alias("order_count"),
            round(sum("amount"), 2).alias("total_revenue"),
            round(avg("amount"), 2).alias("avg_order_value")
        )
        .orderBy(col("total_revenue").desc())
    )

    print("=== Global Country Revenue Summary ===")
    country_summary.show()

    # Step 5: Register catalog view for SQL consumers
    country_summary.createOrReplaceTempView("v_country_revenue")

    print(f"Capstone Pipeline Completed! {country_summary.count()} country marts generated.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson12: Lesson = {
  id: 112,
  title: 'Spark Lab 12 · Basics Capstone: Batch Aggregator',
  concept: `**Congratulations on reaching the Spark Basics Capstone!**

Take a look at \`batch_pipeline.py\`. This pipeline orchestrates everything you have mastered across the basics phase:
1. **Cluster Initialization**: Configures and manages the \`SparkSession\`.
2. **Multi-Source Ingestion**: Reads structured CSV datasets with automated schema inference.
3. **Data Quality & Sanitation**: Drops null foreign keys with \`dropna()\` and isolates completed transactions with narrow \`filter()\` transformations.
4. **Relational Joining**: Joins users and orders on \`user_id\` across partition boundaries.
5. **Two-Phase Distributed Aggregations**: Computes multi-metric sums and averages using \`groupBy()\` and \`agg()\`.
6. **Catalog Registration**: Registers a temporary catalog view for downstream SQL reporting analysts.

In this capstone lab, you will run the complete pipeline and verify cluster execution!`,
  initialFiles: {
    'batch_pipeline.py': BATCH_PIPELINE_PY,
    'data/users.csv': USERS_CSV,
    'data/orders.csv': ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_capstone',
      prompt: "Review `batch_pipeline.py` to trace the full flow: read → clean → join → aggregate → view.",
      hint: "Check that joined_df is aggregated by country and registered as v_country_revenue.",
      validate: (s) =>
        fileContains(s, 'batch_pipeline.py', 'join') &&
        fileContains(s, 'batch_pipeline.py', 'groupBy("country")'),
    },
    {
      id: 'run_capstone',
      prompt: 'Execute the complete enterprise batch pipeline: `spark-submit batch_pipeline.py`.',
      hint: 'Run `spark-submit batch_pipeline.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('batch_pipeline') ||
        Boolean(s.lastRun?.args?.includes('batch_pipeline.py')),
    },
  ],
  quiz: {
    question: 'In an end-to-end PySpark batch pipeline, what is the best practice for filtering data before performing a join?',
    options: [
      'Filter data only after performing the join',
      'Filter and clean data as early as possible before the join to minimize shuffle volume',
      'Never filter data in distributed systems',
      'Convert the DataFrame to a list first',
    ],
    correctIndex: 1,
    explanation: 'Filtering data before a join minimizes the number of records that must be serialized and shuffled across the network, saving immense cluster memory and bandwidth.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'users_data', label: 'users.csv', layer: 'source' },
        { id: 'orders_data', label: 'orders.csv', layer: 'source' },
        { id: 'join_stage', label: 'Inner Join (user_id)', layer: 'staging' },
        { id: 'country_marts', label: 'Country Revenue Mart', layer: 'mart' },
      ],
      edges: [
        { source: 'users_data', target: 'join_stage' },
        { source: 'orders_data', target: 'join_stage' },
        { source: 'join_stage', target: 'country_marts' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark Batch Pipelines Guide', url: 'https://spark.apache.org/docs/latest/sql-programming-guide.html' },
  ],
}

export default sparkLesson12
