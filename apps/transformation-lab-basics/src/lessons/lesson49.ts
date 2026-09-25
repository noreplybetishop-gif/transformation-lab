import type { Lesson } from '../engine/types'
import { commandRan, fileContains } from '../engine/validators'
import { RAW_ORDERS_CSV } from './_canonical'

const STG_ORDERS = `select
    id as order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ source('raw', 'orders') }}
`

const FCT_ORDERS = `select
    order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ ref('stg_orders') }}
`

const SEMANTIC_MODELS_YML = `version: 2

semantic_models:
  - name: semantic_orders
    model: ref('fct_orders')
    description: "Orders semantic model providing standardized revenue measures"
    entities:
      - name: order_id
        type: primary
      - name: customer_id
        type: foreign
    dimensions:
      - name: created_at
        type: time
        type_params:
          time_granularity: day
      - name: status
        type: categorical
    measures:
      - name: total_revenue
        description: "Sum of all order amounts"
        agg: sum
        expr: amount
      - name: order_count
        description: "Total number of orders"
        agg: count
        expr: order_id
`

const lesson49: Lesson = {
  id: 49,
  title: 'The dbt Semantic Layer & Metrics',
  concept: `Different business teams often define the same metric differently (e.g. Finance calculates revenue including refunds, while Sales calculates revenue excluding refunds).

The **dbt Semantic Layer** centralizes metric logic as code inside your dbt repository:
\`\`\`yaml
semantic_models:
  - name: semantic_orders
    model: ref('fct_orders')
    entities:
      - name: order_id
        type: primary
    measures:
      - name: total_revenue
        agg: sum
        expr: amount
      - name: order_count
        agg: count
        expr: order_id
\`\`\`
By defining **entities**, **dimensions**, and **measures** directly on top of dbt models, any downstream tool (Tableau, PowerBI, Hex, Google Sheets) queries the MetricFlow engine to generate identical, governed SQL.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: orders\n`,
    'models/staging/stg_orders.sql': STG_ORDERS,
    'models/marts/fct_orders.sql': FCT_ORDERS,
    'models/semantic_models.yml': SEMANTIC_MODELS_YML,
  },
  seeds: {
    'raw.orders': RAW_ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_semantic_model',
      prompt: "Inspect `models/semantic_models.yml` to verify that `semantic_models:`, `entities:`, and `measures:` are defined.",
      hint: "Check that measures: total_revenue and order_count are configured.",
      validate: (s) =>
        fileContains(s, 'models/semantic_models.yml', 'semantic_models') &&
        fileContains(s, 'models/semantic_models.yml', 'total_revenue'),
    },
    {
      id: 'compile_semantic_dag',
      prompt: 'Execute `dbt compile` to validate the semantic model references against the DAG.',
      hint: 'Type `dbt compile` in the terminal.',
      validate: (s) => commandRan(s, 'compile') || commandRan(s, 'build'),
    },
  ],
  quiz: {
    question: 'What is the primary objective of the dbt Semantic Layer?',
    options: [
      'To convert SQL to NoSQL JSON databases',
      'To provide a single, version-controlled source of truth for business metrics across all downstream BI and reporting tools',
      'To delete unused tables in the data lake',
      'To replace Git with an automated bot',
    ],
    correctIndex: 1,
    explanation: 'The Semantic Layer unifies metric definitions (like revenue or active users) in code so different BI tools and business teams always compute metrics consistently.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.orders', label: 'raw.orders', layer: 'source' },
        { id: 'stg_orders', label: 'stg_orders', layer: 'staging' },
        { id: 'fct_orders', label: 'fct_orders', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.orders', target: 'stg_orders' },
        { source: 'stg_orders', target: 'fct_orders' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Semantic Layer', url: 'https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl' },
  ],
}

export default lesson49
