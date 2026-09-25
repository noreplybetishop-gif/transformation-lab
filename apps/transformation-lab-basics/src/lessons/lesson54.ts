import type { Lesson } from '../engine/types'
import { modelRan, modelMaterialization } from '../engine/validators'
import { RAW_CUSTOMERS_CSV } from './_canonical'

const STG_FAST_CUSTOMERS = `{{ config(
    materialized='view'
) }}

select
    id as customer_id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}
`

const lesson54: Lesson = {
  id: 54,
  title: 'Custom Materializations Concept',
  concept: `Have you ever wondered what actually happens when you write \`{{ config(materialized='view') }}\`?

In dbt, materializations are not hardcoded in Python; **they are Jinja macros**! For example, when you target DuckDB or Postgres, dbt invokes \`materialization_view_duckdb()\` or \`materialization_table_default()\`.

A materialization macro:
1. Resolves relation identifiers (\`target_relation\`).
2. Drops existing database objects if necessary.
3. Generates the appropriate DDL statement (\`CREATE VIEW {{ target_relation }} AS ...\`).
4. Updates relation tracking.

Understanding this architecture enables advanced analytics engineering teams to build custom materializations for Iceberg tables, materialized views with automated refresh intervals, or streaming topics.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: customers\n`,
    'models/staging/stg_fast_customers.sql': STG_FAST_CUSTOMERS,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  tasks: [
    {
      id: 'verify_materialization',
      prompt: "Inspect `stg_fast_customers.sql` and verify it specifies `materialized='view'`.",
      hint: "Check config(materialized='view').",
      validate: (s) => modelMaterialization(s, 'stg_fast_customers', 'view'),
    },
    {
      id: 'run_view',
      prompt: 'Execute `dbt run --select stg_fast_customers` to execute the view materialization pipeline.',
      hint: 'Run `dbt run --select stg_fast_customers`.',
      validate: (s) => modelRan(s, 'stg_fast_customers'),
    },
  ],
  quiz: {
    question: 'How are table and view materializations implemented inside the dbt-core engine?',
    options: [
      'As compiled C++ binaries that cannot be altered',
      'As Jinja macros using the materialization keyword that generate target DDL statements',
      'As bash shell scripts running in docker containers',
      'As JSON configuration files without SQL',
    ],
    correctIndex: 1,
    explanation: 'dbt implements materializations as Jinja macros (defined with {% materialization name, default %}), giving teams total freedom to inspect, customize, or override DDL behavior.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'stg_fast_customers', label: 'stg_fast_customers', layer: 'staging' },
      ],
      edges: [
        { source: 'raw.customers', target: 'stg_fast_customers' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Creating new materializations', url: 'https://docs.getdbt.com/guides/advanced/creating-new-materializations' },
  ],
}

export default lesson54
