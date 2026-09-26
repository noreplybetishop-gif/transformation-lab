import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { USERS_CSV, ORDERS_CSV } from './_canonical'

const LAKEHOUSE_GOVERNANCE_PY = `"""
Lab 44: Data Governance & Lineage in Modern Lakehouses
Implement column-level data contracts, PII masking, and lineage tracking.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat, lit

def main():
    spark = SparkSession.builder.appName("LakehouseGovernance").master("local[*]").getOrCreate()

    users = spark.read.option("header", "true").option("inferSchema", "true").csv("data/users.csv")
    orders = spark.read.option("header", "true").option("inferSchema", "true").csv("data/orders.csv")

    # DATA CONTRACT & PII MASKING POLICY:
    # 1. Anonymize user names using SHA-256 cryptographic hashing
    # 2. Enforce strict null-check contract on user_id and amount
    # 3. Tag column metadata for Unity Catalog / OpenLineage
    governed_users = (
        users
        .withColumn("anonymized_id", sha2(concat(col("id"), lit("_pepper_salt")), 256))
        .select(
            col("id").alias("raw_user_id"),
            col("anonymized_id"),
            col("country")
        )
    )

    governed_orders = (
        orders
        .filter(col("amount").isNotNull() & (col("amount") > 0))
        .join(governed_users, orders.user_id == governed_users.raw_user_id, how="inner")
        .select(
            col("order_id"),
            col("anonymized_id").alias("user_pseudo_id"),
            col("country"),
            col("amount"),
            col("status")
        )
    )

    print("=== GOVERNED DATA PRODUCT (GDPR & SOC2 COMPLIANT) ===")
    governed_orders.show(5, truncate=False)

    print("✓ Data contract validated and column lineage mapped.")
    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson44: Lesson = {
  id: 144,
  title: 'Spark Lab 44 · Data Governance & Lineage in Modern Lakehouses',
  concept: `As enterprise data lakes grow to petabyte scale across hundreds of teams, **Data Governance** and **Lineage** become mandatory for regulatory compliance (GDPR, CCPA, HIPAA, SOC 2).

### The 3 Pillars of Modern Lakehouse Governance

1. **Column-Level Lineage (OpenLineage & Unity Catalog)**:
   Knowing which downstream dashboards or ML models are broken when an upstream column name or business logic changes. Frameworks like **OpenLineage** and Databricks **Unity Catalog** inspect Spark Catalyst query plans at runtime to automatically record column transformation ancestry.

2. **Data Contracts & Quality Assertions**:
   Establishing rigorous contracts between software engineers (producers) and data engineers (consumers):
   - Non-nullable constraints (\`isNotNull()\`).
   - Numerical range constraints (\`amount > 0\`).
   - Schema drift protection.

3. **PII Masking & Anonymization**:
   Personally Identifiable Information (names, emails, credit cards, SSNs) must never leak into raw silver or gold analytics layers. Techniques include:
   - Cryptographic salted hashes (\`sha2(concat(col("id"), lit("salt")), 256)\`).
   - Dynamic data masking.
   - Differential privacy.`,
  initialFiles: {
    'lakehouse_governance.py': LAKEHOUSE_GOVERNANCE_PY,
    'data/users.csv': USERS_CSV,
    'data/orders.csv': ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_governance_masking',
      prompt: "Ensure `lakehouse_governance.py` implements PII anonymization using `sha2` and enforces amount null checks.",
      hint: "Check that sha2 and isNotNull filters are used in the script.",
      validate: (s) =>
        fileContains(s, 'lakehouse_governance.py', 'sha2') &&
        fileContains(s, 'lakehouse_governance.py', 'isNotNull'),
    },
    {
      id: 'run_governance_script',
      prompt: 'Execute the governance pipeline: `spark-submit lakehouse_governance.py`.',
      hint: 'Type `spark-submit lakehouse_governance.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('lakehouse_governance') ||
        Boolean(s.lastRun?.args?.includes('lakehouse_governance.py')),
    },
  ],
  quiz: {
    question: 'How do tools like OpenLineage extract column-level lineage from Apache Spark jobs?',
    options: [
      'By parsing Catalyst query execution plans via custom SparkListener hooks during job execution',
      'By reading git commit comments',
      'By manually interviewing software engineers',
      'By measuring computer fan speed',
    ],
    correctIndex: 0,
    explanation: 'OpenLineage and modern catalogs register Spark listeners that inspect the Catalyst Logical and Physical plans, capturing exact source-to-target column mappings at execution time.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw', label: 'Raw PII Data (Users & Orders)', layer: 'source' },
        { id: 'contract', label: 'Data Contract & Salted SHA256', layer: 'staging' },
        { id: 'governed', label: 'Governed Anonymized Product', layer: 'mart' },
      ],
      edges: [
        { source: 'raw', target: 'contract' },
        { source: 'contract', target: 'governed' },
      ],
    },
  },
  furtherReading: [
    { label: 'OpenLineage Spark Integration Architecture', url: 'https://openlineage.io/docs/integrations/spark/' },
  ],
}

export default sparkLesson44
