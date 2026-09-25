import type { Lesson } from '../engine/types'
import { modelCompiled, fileContains } from '../engine/validators'
import { RAW_ORDERS_CSV } from './_canonical'

const STG_ORDERS = `select
    id as order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ source('raw', 'orders') }}
`

const lesson38: Lesson = {
  id: 38,
  title: 'Incremental Predicates & Partition Pruning',
  concept: `When merging into a multi-terabyte table partitioned by date, checking the entire destination table for matches causes full-table scans that cost thousands of dollars and minutes of runtime.

\`incremental_predicates\` solves this by injecting custom SQL predicates directly into the join or merge condition:
\`\`\`sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_predicates=[
      "DBT_INTERNAL_DEST.created_at >= dateadd('day', -7, current_date)"
    ]
) }}
\`\`\`
The warehouse's query planner uses this predicate to prune untouched partitions, restricting the merge search to only the relevant date window.

In this lab, you will configure \`incremental_predicates\` and compile the model to inspect dbt's partition-pruning configuration.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: orders\n`,
    'models/staging/stg_orders.sql': STG_ORDERS,
    'models/marts/fct_orders_predicates.sql': `{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select
    order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ ref('stg_orders') }}
`,
  },
  seeds: {
    'raw.orders': RAW_ORDERS_CSV,
  },
  tasks: [
    {
      id: 'add_predicates',
      prompt: "Add `incremental_predicates` to the config block of `fct_orders_predicates.sql` to restrict target partition lookups.",
      hint: "Add: incremental_predicates=[\"DBT_INTERNAL_DEST.created_at >= '2024-01-01'\"] inside config().",
      validate: (s) =>
        fileContains(s, 'models/marts/fct_orders_predicates.sql', 'incremental_predicates'),
    },
    {
      id: 'compile_model',
      prompt: 'Execute `dbt compile --select fct_orders_predicates` to compile and inspect the query.',
      hint: 'Type `dbt compile --select fct_orders_predicates` in the terminal.',
      validate: (s) => modelCompiled(s, 'fct_orders_predicates'),
    },
  ],
  quiz: {
    question: 'How do incremental_predicates improve performance during warehouse merge operations?',
    options: [
      'They prevent other users from logging into the warehouse',
      'They prune partitions in the destination table, preventing full table scans during key resolution',
      'They convert all string columns to integer types',
      'They skip running dbt tests automatically',
    ],
    correctIndex: 1,
    explanation: 'By telling the query optimizer which partition range (e.g. last 7 days) can possibly match incoming records, the engine skips scanning past partitions entirely.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.orders', label: 'raw.orders', layer: 'source' },
        { id: 'stg_orders', label: 'stg_orders', layer: 'staging' },
        { id: 'fct_orders_predicates', label: 'fct_orders_predicates', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.orders', target: 'stg_orders' },
        { source: 'stg_orders', target: 'fct_orders_predicates' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt incremental predicates', url: 'https://docs.getdbt.com/docs/build/incremental-strategy#about-incremental_predicates' },
  ],
}

export default lesson38
