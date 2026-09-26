import type { Lesson } from '../../engine/types'
import { fileContains } from '../../engine/validators'
import { NESTED_USERS_JSON } from './_canonical'

const NESTED_STRUCTURES_PY = `"""
Lab 15: Handling Arrays, Structs & Maps in PySpark
Parse, unpack, and manipulate complex nested JSON documents.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, struct, array_contains

def main():
    spark = SparkSession.builder.appName("ComplexTypesLab").getOrCreate()

    # Ingest nested JSON data
    users_df = spark.read.json("data/nested_users.json")

    print("=== Raw Nested Schema ===")
    users_df.printSchema()

    # 1. Accessing Struct fields with dot notation (address.city)
    # 2. Filtering on Arrays with array_contains()
    premium_users = users_df.filter(array_contains(col("tags"), "premium"))

    # 3. Exploding arrays: Flatten 1-to-many tags into individual rows
    flattened_tags = (
        users_df
        .select(
            col("id"),
            col("name"),
            col("address.city").alias("city"),
            explode(col("tags")).alias("tag")
        )
    )

    print("=== Flattened User Tags ===")
    flattened_tags.show(10)

    spark.stop()

if __name__ == "__main__":
    main()
`

const sparkLesson15: Lesson = {
  id: 115,
  title: 'Spark Lab 15 · Handling Arrays, Structs & Maps',
  concept: `Modern data engineering frequently encounters nested semi-structured data (REST APIs, Kafka events, NoSQL exports). PySpark provides first-class support for complex types:

### 1. Structs (\`StructType\`)
A fixed group of named fields (like an embedded record).
- Access nested fields with dot notation: \`col("address.city")\` or \`df["address"]["city"]\`.
- Create structs on the fly: \`struct(col("city"), col("zip")).alias("geo_info")\`.

### 2. Arrays (\`ArrayType\`)
An ordered collection of elements of the same type.
- Test membership: \`array_contains(col("tags"), "vip")\`.
- Explode: \`explode(col("tags"))\` transforms an array of $N$ items into $N$ separate rows!

### 3. Maps (\`MapType\`)
Key-value lookup dictionaries.
- Access value: \`col("prefs")["newsletter"]\`.

In this lab, you will explore nested JSON payloads, query nested structs with dot notation, and explode array tags.`,
  initialFiles: {
    'nested_structures.py': NESTED_STRUCTURES_PY,
    'data/nested_users.json': NESTED_USERS_JSON,
  },
  tasks: [
    {
      id: 'verify_nested_code',
      prompt: "Inspect `nested_structures.py` to confirm the use of `explode` and `array_contains`.",
      hint: "Check that explode and array_contains are imported and used in the DataFrame pipeline.",
      validate: (s) =>
        fileContains(s, 'nested_structures.py', 'explode') &&
        fileContains(s, 'nested_structures.py', 'array_contains'),
    },
    {
      id: 'run_nested_job',
      prompt: 'Execute the nested structures pipeline: `spark-submit nested_structures.py`.',
      hint: 'Run `spark-submit nested_structures.py` in the terminal.',
      validate: (s) =>
        s.ranModels.has('nested_structures') ||
        Boolean(s.lastRun?.args?.includes('nested_structures.py')),
    },
  ],
  quiz: {
    question: 'What is the effect of calling the `explode()` function on an Array column containing 3 elements for a given row?',
    options: [
      'It converts the array into a comma-delimited string',
      'It duplicates the parent row 3 times, each with one element from the array',
      'It deletes the row from memory to prevent memory leaks',
      'It calculates the sum of all numerical values in the array',
    ],
    correctIndex: 1,
    explanation: '`explode()` takes an array of size N and outputs N separate rows, retaining the values of all other projected columns for each unpacked element.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'json_source', label: 'nested_users.json', layer: 'source' },
        { id: 'struct_projection', label: 'Struct Projection (address.city)', layer: 'staging' },
        { id: 'explode_stage', label: 'Explode Array (tags)', layer: 'mart' },
      ],
      edges: [
        { source: 'json_source', target: 'struct_projection' },
        { source: 'struct_projection', target: 'explode_stage' },
      ],
    },
  },
  furtherReading: [
    { label: 'PySpark SQL Functions - explode', url: 'https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html' },
    { label: 'Working with Complex Data Types in Spark', url: 'https://spark.apache.org/docs/latest/sql-ref-datatypes.html' },
  ],
}

export default sparkLesson15
