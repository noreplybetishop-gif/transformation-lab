import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { USERS_CSV, ORDERS_CSV, PRODUCTS_CSV } from './_canonical'

const LAKEHOUSE_CAPSTONE_PY = `"""
Lab 27: Intermediate Capstone: E-Commerce Lakehouse
Orchestrate an end-to-end Medallion Lakehouse (Bronze -> Silver -> Gold) using Delta Lake.
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, round, sum, count, broadcast, row_number, to_date

def main():
    spark = (
        SparkSession.builder
        .appName("LakehouseCapstone")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    print("==================================================")
    print("STEP 1: BRONZE LAYER (Raw Ingestion into Delta)")
    print("==================================================")
    raw_orders = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")
    raw_users = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv")
    raw_products = spark.read.option("header", "true").option("inferSchema", "true").csv("data/products.csv")

    raw_orders.write.format("delta").mode("overwrite").save("lakehouse/bronze/orders")
    raw_users.write.format("delta").mode("overwrite").save("lakehouse/bronze/users")
    raw_products.write.format("delta").mode("overwrite").save("lakehouse/bronze/products")
    print("✓ Bronze Tables Committed Atomically.")

    print("==================================================")
    print("STEP 2: SILVER LAYER (Clean, Join & Enrich)")
    print("==================================================")
    b_orders = spark.read.format("delta").load("lakehouse/bronze/orders").alias("o")
    b_users = spark.read.format("delta").load("lakehouse/bronze/users").alias("u")
    b_products = spark.read.format("delta").load("lakehouse/bronze/products").alias("p")

    # Clean & enrich orders with user and product metadata
    fact_orders = b_orders.withColumn("product_id", (col("o.order_id") % 6) + 101)

    silver_enriched_orders = (
        fact_orders
        .filter(col("o.status") == "COMPLETED")
        .join(b_users, col("o.user_id") == col("u.id"), how="inner")
        .join(broadcast(b_products), col("fact_orders.product_id") == col("p.product_id"), how="left")
        .select(
            col("o.order_id"),
            col("o.user_id"),
            col("u.name").alias("user_name"),
            col("u.country"),
            col("p.name").alias("product_name"),
            col("p.category"),
            col("o.amount"),
            to_date(col("o.created_at")).alias("order_date")
        )
    )

    silver_enriched_orders.write.format("delta").mode("overwrite").partitionBy("country").save("lakehouse/silver/enriched_orders")
    print(f"✓ Silver Enriched Orders Saved: {silver_enriched_orders.count()} records.")

    print("==================================================")
    print("STEP 3: GOLD LAYER (Business KPIs & Window Ranks)")
    print("==================================================")
    silver_df = spark.read.format("delta").load("lakehouse/silver/enriched_orders")

    # Compute category revenue rankings per country
    country_cat_window = Window.partitionBy("country").orderBy(col("total_revenue").desc())

    gold_kpis = (
        silver_df
        .groupBy("country", "category")
        .agg(
            round(sum("amount"), 2).alias("total_revenue"),
            count("order_id").alias("order_count")
        )
        .withColumn("category_rank", row_number().over(country_cat_window))
        .orderBy(col("country"), col("category_rank"))
    )

    gold_kpis.write.format("delta").mode("overwrite").save("lakehouse/gold/country_category_kpis")
    print("✓ Gold Executive KPI Mart Published:")
    gold_kpis.show()

    print("🎉 Lakehouse Capstone Pipeline Completed Successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson27: Lesson = {
  id: 127,
  title: 'Spark Lab 27 · Intermediate Capstone: E-Commerce Lakehouse',
  concept: `**Congratulations on reaching the Spark Intermediate Capstone!**

Take a look at \`lakehouse_capstone.py\`. This production pipeline builds a real **Medallion Architecture (Bronze -> Silver -> Gold)** on Delta Lake:

### 1. Bronze Layer (Raw Ingestion)
- Ingests raw CSV source feeds into append-only Delta Lake tables with zero transformation.
- Guarantees an immutable audit trail with full time-travel rollback capability.

### 2. Silver Layer (Cleaned & Curated)
- Filters invalid/cancelled records.
- Performs multi-way joins with **Broadcast Hash Joins** for dimension tables.
- Standardizes data types (\`to_date\`) and partitions physical Parquet files by \`country\`.

### 3. Gold Layer (Aggregated Business Marts)
- Computes executive analytics and KPI tables.
- Leverages analytical **Window specifications** (\`row_number().over(...)\`) to rank product categories by revenue per region.

In this capstone, you will run the complete pipeline and verify the generation of all three medallion tiers!`,
  initialFiles: {
    'lakehouse_capstone.py': LAKEHOUSE_CAPSTONE_PY,
    'data/users.csv': USERS_CSV,
    'data/orders.csv': ORDERS_CSV,
    'data/products.csv': PRODUCTS_CSV,
  },
  tasks: [
    {
      id: 'verify_capstone_pipeline',
      prompt: "Inspect `lakehouse_capstone.py` to confirm the Bronze, Silver, and Gold layers with Delta Lake and Window ranking.",
      hint: "Check that raw ingestion, broadcast joins, and windowed row_number rankings are orchestrated.",
      validate: (s) =>
        fileContains(s, 'lakehouse_capstone.py', 'lakehouse/bronze') &&
        fileContains(s, 'lakehouse_capstone.py', 'lakehouse/silver') &&
        fileContains(s, 'lakehouse_capstone.py', 'lakehouse/gold'),
    },
    {
      id: 'run_capstone_pipeline',
      prompt: 'Execute the complete Lakehouse Capstone: `spark-submit lakehouse_capstone.py`.',
      hint: 'Type `spark-submit lakehouse_capstone.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('lakehouse_capstone') ||
        Boolean(s.lastRun?.args?.includes('lakehouse_capstone.py')),
    },
  ],
  quiz: {
    question: 'In the Medallion Lakehouse architecture, what is the primary role of the Silver layer?',
    options: [
      'Raw unvalidated dump of raw log files',
      'Cleaned, enriched, joined, and validated data ready for ad-hoc exploration and downstream aggregation',
      'Discarding all historical logs to save disk space',
      'Only storing static image files',
    ],
    correctIndex: 1,
    explanation: 'The Silver layer transforms raw Bronze data into a trusted enterprise view by filtering errors, joining dimension lookups, and enforcing consistent schemas.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'bronze', label: 'Bronze Layer (Raw Delta)', layer: 'source' },
        { id: 'silver', label: 'Silver Layer (Enriched & Partitioned)', layer: 'staging' },
        { id: 'gold', label: 'Gold Layer (Windowed Business KPIs)', layer: 'mart' },
      ],
      edges: [
        { source: 'bronze', target: 'silver' },
        { source: 'silver', target: 'gold' },
      ],
    },
  },
  furtherReading: [
    { label: 'Databricks Medallion Architecture Overview', url: 'https://www.databricks.com/glossary/medallion-architecture' },
    { label: 'Building Reliable Data Lakes with Delta Lake', url: 'https://docs.delta.io/' },
  ],
}

export default sparkLesson27
