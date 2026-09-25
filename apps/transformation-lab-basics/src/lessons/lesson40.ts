import type { Lesson } from '../engine/types'
import { modelCompiled, modelMaterialization } from '../engine/validators'
import { RAW_ORDERS_CSV, RAW_ORDER_ITEMS_CSV } from './_canonical'

const STG_ORDER_ITEMS = `{{ config(materialized='view') }}

select
    id as item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price as item_total
from {{ source('raw', 'order_items') }}
`

const FCT_ORDERS_WITH_ITEMS = `select
    o.id as order_id,
    o.customer_id,
    o.amount,
    sum(items.quantity) as total_items,
    sum(items.item_total) as calculated_total
from {{ source('raw', 'orders') }} o
left join {{ ref('stg_order_items_ephemeral') }} items
    on o.id = items.order_id
group by o.id, o.customer_id, o.amount
`

const lesson40: Lesson = {
  id: 40,
  title: 'Ephemeral Materialization & CTE Inlining',
  concept: `Sometimes you want to keep your transformation modular without cluttering the database catalog with dozens of micro-views or intermediate tables that no BI user will ever query.

Enter **\`materialized='ephemeral'\`**:
\`\`\`sql
{{ config(materialized='ephemeral') }}
\`\`\`
An ephemeral model is never created as a physical database object. Instead, when another model calls \`ref('my_ephemeral_model')\`, dbt seamlessly injects the ephemeral model's SQL as a Common Table Expression (\`WITH __dbt__cte__... AS (...)\`) directly into the compiled query!

This maintains modular code organization in your git repository while reducing database catalog clutter.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: orders\n      - name: order_items\n`,
    'models/staging/stg_order_items_ephemeral.sql': STG_ORDER_ITEMS,
    'models/marts/fct_orders_with_items.sql': FCT_ORDERS_WITH_ITEMS,
  },
  seeds: {
    'raw.orders': RAW_ORDERS_CSV,
    'raw.order_items': RAW_ORDER_ITEMS_CSV,
  },
  tasks: [
    {
      id: 'make_ephemeral',
      prompt: "Change the materialization in `stg_order_items_ephemeral.sql` to `ephemeral`.",
      hint: "Change materialized='view' to materialized='ephemeral'.",
      validate: (s) => modelMaterialization(s, 'stg_order_items_ephemeral', 'ephemeral'),
    },
    {
      id: 'compile_downstream',
      prompt: 'Run `dbt compile --select fct_orders_with_items` to see how dbt inlines the CTE into the compiled query.',
      hint: 'Run `dbt compile --select fct_orders_with_items`.',
      validate: (s) => modelCompiled(s, 'fct_orders_with_items'),
    },
  ],
  quiz: {
    question: 'Where is an ephemeral model physically stored in the warehouse database?',
    options: [
      'In a temporary scratch schema',
      'Nowhere - it only exists as an inlined CTE inside the queries of downstream models that ref() it',
      'In the warehouse master log file',
      'In a memory-cached table',
    ],
    correctIndex: 1,
    explanation: 'Ephemeral models produce zero database objects. dbt interpolates their compiled SQL into downstream models as Common Table Expressions (WITH clauses).',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.orders', label: 'raw.orders', layer: 'source' },
        { id: 'raw.order_items', label: 'raw.order_items', layer: 'source' },
        { id: 'stg_order_items_ephemeral', label: 'stg_order_items_ephemeral (ephemeral)', layer: 'intermediate' },
        { id: 'fct_orders_with_items', label: 'fct_orders_with_items', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.order_items', target: 'stg_order_items_ephemeral' },
        { source: 'raw.orders', target: 'fct_orders_with_items' },
        { source: 'stg_order_items_ephemeral', target: 'fct_orders_with_items' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Ephemeral Materialization', url: 'https://docs.getdbt.com/docs/build/materializations#ephemeral' },
  ],
}

export default lesson40
