import type { Lesson } from '../engine/types'
import { modelRan, modelSqlMatches, fileContains } from '../engine/validators'
import { RAW_CUSTOMERS_CSV } from './_canonical'

const STG_CUSTOMERS = `select
    id as customer_id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}
`

const lesson35: Lesson = {
  id: 35,
  title: 'Incremental Merge Strategy',
  concept: `Welcome to the **Advanced Course**! We begin with advanced performance engineering for massive datasets.

By default on many database adapters, incremental models use a merge join or insert-overwrite. In dbt, setting:
\`\`\`sql
{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}
\`\`\`
instructs dbt to generate an atomic \`MERGE INTO\` SQL statement against your target table. When a matching \`unique_key\` exists, existing rows are updated; when no match exists, new rows are inserted.

In this lab, you will configure an incremental dimension model using the \`merge\` strategy and execute a build.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: customers\n`,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
    'models/marts/dim_customers_merge.sql': `-- Configure incremental materialized with unique_key and merge strategy\n\nselect\n    customer_id,\n    name,\n    email,\n    country\nfrom {{ ref('stg_customers') }}\n`,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  tasks: [
    {
      id: 'configure_merge',
      prompt: "Configure `dim_customers_merge.sql` with `materialized='incremental'`, `unique_key='customer_id'`, and `incremental_strategy='merge'`.",
      hint: "Add {{ config(materialized='incremental', unique_key='customer_id', incremental_strategy='merge') }} at the top.",
      validate: (s) =>
        fileContains(s, 'models/marts/dim_customers_merge.sql', 'incremental_strategy') &&
        modelSqlMatches(s, 'dim_customers_merge', /incremental_strategy\s*=\s*['"]merge['"]/),
    },
    {
      id: 'run_model',
      prompt: 'Execute `dbt run --select dim_customers_merge` to build the merged incremental table.',
      hint: 'Run `dbt run --select dim_customers_merge`.',
      validate: (s) => modelRan(s, 'dim_customers_merge'),
    },
  ],
  quiz: {
    question: 'How does dbt handle an incoming row when using incremental_strategy="merge" with a unique_key?',
    options: [
      'It always appends the row regardless of whether the key exists',
      'It updates existing target rows matching the key, and inserts new ones',
      'It drops and recreates the table on every run',
      'It deletes all rows in the destination table first',
    ],
    correctIndex: 1,
    explanation: 'The merge strategy translates into a MERGE INTO SQL statement: matching keys are updated with new column values (WHEN MATCHED THEN UPDATE), while novel keys are inserted (WHEN NOT MATCHED THEN INSERT).',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'stg_customers', label: 'stg_customers', layer: 'staging' },
        { id: 'dim_customers_merge', label: 'dim_customers_merge', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.customers', target: 'stg_customers' },
        { source: 'stg_customers', target: 'dim_customers_merge' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Incremental Strategies', url: 'https://docs.getdbt.com/docs/build/incremental-strategy' },
  ],
}

export default lesson35
