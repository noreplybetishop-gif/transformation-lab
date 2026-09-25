import type { Lesson } from '../engine/types'
import { modelRan, modelSqlMatches, fileContains } from '../engine/validators'
import { RAW_ORDERS_CSV } from './_canonical'

const STG_ORDERS = `select
    id as order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ source('raw', 'orders') }}
`

const lesson37: Lesson = {
  id: 37,
  title: 'Delete+Insert Strategy',
  concept: `On several warehouse engines (including BigQuery without partitioning, Postgres, or Redshift), \`MERGE\` statements can trigger row locks or complete table rewrites.

The \`delete+insert\` strategy provides a rock-solid alternative:
\`\`\`sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_id'
) }}
\`\`\`
During each incremental run, dbt stages incoming records in a temporary table, executes a targeted \`DELETE FROM target WHERE key IN (SELECT key FROM temp)\`, and then runs an \`INSERT INTO target SELECT * FROM temp\`.

This avoids MERGE locking issues and guarantees deterministic deduplication.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: orders\n`,
    'models/staging/stg_orders.sql': STG_ORDERS,
    'models/marts/fct_orders_delete_insert.sql': `-- Configure delete+insert strategy with unique_key\n\nselect\n    order_id,\n    customer_id,\n    amount,\n    status,\n    created_at\nfrom {{ ref('stg_orders') }}\n`,
  },
  seeds: {
    'raw.orders': RAW_ORDERS_CSV,
  },
  tasks: [
    {
      id: 'configure_delete_insert',
      prompt: "Configure `fct_orders_delete_insert.sql` with `materialized='incremental'`, `incremental_strategy='delete+insert'`, and `unique_key='order_id'`.",
      hint: "Add {{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='order_id') }} at the top.",
      validate: (s) =>
        fileContains(s, 'models/marts/fct_orders_delete_insert.sql', 'delete+insert') &&
        modelSqlMatches(s, 'fct_orders_delete_insert', /incremental_strategy\s*=\s*['"]delete\+insert['"]/),
    },
    {
      id: 'run_model',
      prompt: 'Execute `dbt run --select fct_orders_delete_insert`.',
      hint: 'Type `dbt run --select fct_orders_delete_insert` in the console.',
      validate: (s) => modelRan(s, 'fct_orders_delete_insert'),
    },
  ],
  quiz: {
    question: 'What two operations does dbt execute under the hood with incremental_strategy="delete+insert"?',
    options: [
      'Drops the database and rebuilds all views',
      'Deletes matching keys from the destination table, then inserts the new rows from staging',
      'Updates row metadata without writing table data',
      'Renames the staging table to become the destination table',
    ],
    correctIndex: 1,
    explanation: 'dbt identifies existing rows sharing the unique_key, removes them from the destination with a DELETE statement, and then executes an INSERT to insert the updated rows.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.orders', label: 'raw.orders', layer: 'source' },
        { id: 'stg_orders', label: 'stg_orders', layer: 'staging' },
        { id: 'fct_orders_delete_insert', label: 'fct_orders_delete_insert', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.orders', target: 'stg_orders' },
        { source: 'stg_orders', target: 'fct_orders_delete_insert' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt delete+insert strategy', url: 'https://docs.getdbt.com/docs/build/incremental-strategy#deleteinsert' },
  ],
}

export default lesson37
