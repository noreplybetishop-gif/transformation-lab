import type { Lesson } from '../engine/types'
import { modelSqlMatches, modelRan, modelShown } from '../engine/validators'
import { RAW_SUBSCRIPTIONS_CSV } from './_canonical'

const SOURCES_YML = `version: 2

sources:
  - name: raw
    tables:
      - name: subscriptions
`

const SNAP_SUBSCRIPTIONS = `{% snapshot snap_customer_subscriptions %}

{{
    config(
      target_schema='main',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('raw', 'subscriptions') }}

{% endsnapshot %}
`

const DIM_CURRENT_SUBSCRIPTIONS = `select
    id as subscription_id,
    customer_id,
    plan,
    status
from {{ ref('snap_customer_subscriptions') }}
`

const lesson22: Lesson = {
  id: 22,
  title: 'Querying Snapshot Dimensions',
  concept: `Now that your snapshot is capturing changes over time, how do downstream marts and BI dashboards query it?

There are two primary query patterns for SCD Type 2 tables:

### 1. Current State (Active Snapshot)
Most marts only care about the **current** status of each customer. To filter for the present state, add:

\`\`\`sql
where dbt_valid_to is null
\`\`\`

Because dbt sets \`dbt_valid_to = NULL\` on all currently active records, this filter guarantees you receive exactly one row per entity with zero duplicates.

### 2. Point-in-Time ("As-of") Join
When joining dimensions to transactions (e.g. associating an order with the user's subscription plan on the day the order took place), join using the validity window:

\`\`\`sql
left join {{ ref('snap_customer_subscriptions') }} sub
  on orders.customer_id = sub.customer_id
 and orders.created_at >= sub.dbt_valid_from
 and (orders.created_at < sub.dbt_valid_to or sub.dbt_valid_to is null)
\`\`\`

In this lab, complete \`dim_current_subscriptions\` to select only the current active records.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'snapshots/snap_customer_subscriptions.sql': SNAP_SUBSCRIPTIONS,
    'models/marts/dim_current_subscriptions.sql': DIM_CURRENT_SUBSCRIPTIONS,
  },
  seeds: {
    'raw.subscriptions': RAW_SUBSCRIPTIONS_CSV,
  },
  preRanModels: ['snap_customer_subscriptions'],
  tasks: [
    {
      id: 'valid_to_null',
      prompt: "Add `where dbt_valid_to is null` to `models/marts/dim_current_subscriptions.sql`.",
      hint: "Add `where dbt_valid_to is null` at the bottom of the SQL query.",
      validate: (s) =>
        modelSqlMatches(s, 'dim_current_subscriptions', /where\s+dbt_valid_to\s+is\s+null/i),
    },
    {
      id: 'run',
      prompt: 'Run `dbt run --select dim_current_subscriptions` to materialize the current dimension model.',
      hint: 'Type `dbt run --select dim_current_subscriptions`.',
      validate: (s) => modelRan(s, 'dim_current_subscriptions'),
    },
    {
      id: 'show',
      prompt: 'Preview the materialized current dimensions with `dbt show --select dim_current_subscriptions`.',
      hint: 'Run `dbt show --select dim_current_subscriptions`.',
      validate: (s) => modelShown(s, 'dim_current_subscriptions'),
    },
  ],
  quiz: {
    question: 'How do you filter a snapshot table to get the current state of all records?',
    options: [
      'WHERE dbt_valid_from = current_date',
      'WHERE dbt_valid_to IS NULL',
      'WHERE dbt_scd_id = 1',
      'WHERE status = "latest"',
    ],
    correctIndex: 1,
    explanation: '`dbt_valid_to IS NULL` selects rows that have not been superseded by a newer version, giving you the latest state of each entity.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.subscriptions', label: 'raw.subscriptions', layer: 'source' },
        { id: 'snap_customer_subscriptions', label: 'snap_customer_subscriptions', layer: 'intermediate' },
        { id: 'dim_current_subscriptions', label: 'dim_current_subscriptions', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.subscriptions', target: 'snap_customer_subscriptions' },
        { source: 'snap_customer_subscriptions', target: 'dim_current_subscriptions' },
      ],
    },
  },
  furtherReading: [
    { label: 'Querying snapshots', url: 'https://docs.getdbt.com/docs/build/snapshots#querying-snapshots' },
    { label: 'Point-in-time joins', url: 'https://docs.getdbt.com/blog/managing-scd-dbt' },
  ],
}

export default lesson22
