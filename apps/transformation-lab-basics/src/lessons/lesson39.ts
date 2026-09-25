import type { Lesson } from '../engine/types'
import { modelRan, modelMaterialization } from '../engine/validators'
import { RAW_PAYMENTS_CSV } from './_canonical'

const STG_PAYMENTS = `select
    id as payment_id,
    order_id,
    payment_method,
    amount_cents / 100.0 as amount
from {{ source('raw', 'payments') }}
`

const lesson39: Lesson = {
  id: 39,
  title: 'Performance Tuning: Materialization Strategies',
  concept: `Choosing between \`view\`, \`table\`, \`incremental\`, and \`ephemeral\` is one of the most impactful architectural decisions in analytics engineering:

- **Views**: Compute on-read. Zero storage cost and always current, but repeated queries recalculate the transformation every single time.
- **Tables**: Pre-compute on-write. Queries are lightning-fast for BI tools and dashboards, but scheduled dbt jobs rewrite the dataset.
- **Incremental**: Best of both worlds for massive append/update streams.

In complex DAGs, long chains of views (\`view -> view -> view\`) cause massive query planning explosion and sluggish dashboard response times. Converting intermediate rollup marts to \`table\` materialization pre-computes results for blazing-fast analyst queries.

In this lab, you will optimize \`fct_financial_summary.sql\` by materializing it as a \`table\`.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: payments\n`,
    'models/staging/stg_payments.sql': STG_PAYMENTS,
    'models/marts/fct_financial_summary.sql': `{{ config(materialized='view') }}

select
    payment_method,
    count(*) as total_transactions,
    sum(amount) as total_volume
from {{ ref('stg_payments') }}
group by payment_method
`,
  },
  seeds: {
    'raw.payments': RAW_PAYMENTS_CSV,
  },
  tasks: [
    {
      id: 'change_materialization',
      prompt: "Update `fct_financial_summary.sql` configuration to `materialized='table'`.",
      hint: "Change materialized='view' to materialized='table'.",
      validate: (s) => modelMaterialization(s, 'fct_financial_summary', 'table'),
    },
    {
      id: 'run_model',
      prompt: 'Execute `dbt run --select fct_financial_summary` to materialize the summary table.',
      hint: 'Run `dbt run --select fct_financial_summary`.',
      validate: (s) => modelRan(s, 'fct_financial_summary'),
    },
  ],
  quiz: {
    question: 'What is the danger of nesting multiple complex views on top of each other in a production warehouse?',
    options: [
      'The warehouse runs out of disk storage space',
      'Query engines must re-execute every upstream transformation upon every user query, degrading dashboard speed',
      'dbt will delete all column comments',
      'Tables become read-only and cannot be altered',
    ],
    correctIndex: 1,
    explanation: 'Views do not store data physically. When multiple views are chained together, any downstream query must evaluate the entire cascade on the fly, creating heavy computational load.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.payments', label: 'raw.payments', layer: 'source' },
        { id: 'stg_payments', label: 'stg_payments', layer: 'staging' },
        { id: 'fct_financial_summary', label: 'fct_financial_summary', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.payments', target: 'stg_payments' },
        { source: 'stg_payments', target: 'fct_financial_summary' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Materializations', url: 'https://docs.getdbt.com/docs/build/materializations' },
  ],
}

export default lesson39
