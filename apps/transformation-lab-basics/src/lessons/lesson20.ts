import type { Lesson } from '../engine/types'
import { snapshotRan, modelShown, fileMatches } from '../engine/validators'
import { RAW_SUBSCRIPTIONS_CSV } from './_canonical'

const SOURCES_YML = `version: 2

sources:
  - name: raw
    tables:
      - name: subscriptions
`

const SNAP_SUBSCRIPTIONS_STARTER = `{% snapshot snap_customer_subscriptions %}

{{
    config(
      target_schema='main',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select
    id,
    customer_id,
    plan,
    status,
    updated_at
from {{ source('raw', 'subscriptions') }}

{% endsnapshot %}
`

const lesson20: Lesson = {
  id: 20,
  title: 'Snapshot Timestamp Strategy',
  concept: `dbt provides two snapshot strategies: **\`timestamp\`** and **\`check\`**.

The **\`timestamp\`** strategy is the industry best practice whenever the source table contains a reliable \`updated_at\`, \`modified_at\`, or \`last_updated\` column.

Why is the timestamp strategy preferred?
1. **Performance**: dbt only compares the record's \`updated_at\` against the maximum \`dbt_updated_at\` recorded in previous snapshots, rather than hashing and comparing every column value.
2. **True Event Time**: The validity window reflects when the change actually occurred in the source application, rather than the arbitrary moment the dbt snapshot job ran.

The required configurations are:
- \`strategy='timestamp'\`
- \`unique_key\`: Primary key of the source entity (e.g., \`'id'\`).
- \`updated_at\`: The timestamp column recording updates (e.g., \`'updated_at'\`).
- \`target_schema\`: Target schema in the warehouse where the snapshot table will be stored.

Review and run this configuration on \`snap_customer_subscriptions\`.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'snapshots/snap_customer_subscriptions.sql': SNAP_SUBSCRIPTIONS_STARTER,
  },
  seeds: {
    'raw.subscriptions': RAW_SUBSCRIPTIONS_CSV,
  },
  tasks: [
    {
      id: 'verify_config',
      prompt: "Ensure `snapshots/snap_customer_subscriptions.sql` configures `strategy='timestamp'`, `unique_key='id'`, and `updated_at='updated_at'`.",
      hint: "Check that `config(...)` has `strategy='timestamp'`, `unique_key='id'`, and `updated_at='updated_at'`.",
      validate: (s) =>
        fileMatches(s, 'snapshots/snap_customer_subscriptions.sql', /strategy\s*=\s*['"]timestamp['"]/i) &&
        fileMatches(s, 'snapshots/snap_customer_subscriptions.sql', /updated_at\s*=\s*['"]updated_at['"]/i),
    },
    {
      id: 'snapshot',
      prompt: 'Run `dbt snapshot` to capture the current state of customer subscriptions.',
      hint: 'Type `dbt snapshot` in the terminal.',
      validate: (s) => snapshotRan(s, 'snap_customer_subscriptions') || s.lastRun?.command === 'snapshot',
    },
    {
      id: 'show',
      prompt: 'Preview the snapshot columns using `dbt show --select snap_customer_subscriptions`.',
      hint: 'Run `dbt show --select snap_customer_subscriptions`.',
      validate: (s) => modelShown(s, 'snap_customer_subscriptions'),
    },
  ],
  quiz: {
    question: 'Why is the timestamp strategy preferred over the check strategy when updated_at is available?',
    options: [
      'The check strategy does not support primary keys',
      'The timestamp strategy is faster and accurately reflects when the application made the change',
      'The check strategy can only track one column at a time',
      'The timestamp strategy automatically deletes old data',
    ],
    correctIndex: 1,
    explanation: 'The `timestamp` strategy is faster because it only checks a single timestamp column, and it accurately records the source system’s event time instead of the batch execution time.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.subscriptions', label: 'raw.subscriptions', layer: 'source' },
        { id: 'snap_customer_subscriptions', label: 'snap_customer_subscriptions', layer: 'intermediate' },
      ],
      edges: [
        { source: 'raw.subscriptions', target: 'snap_customer_subscriptions' },
      ],
    },
  },
  furtherReading: [
    { label: 'Timestamp snapshot strategy', url: 'https://docs.getdbt.com/docs/build/snapshots#timestamp-strategy' },
    { label: 'Snapshot best practices', url: 'https://docs.getdbt.com/docs/build/snapshots#best-practices' },
  ],
}

export default lesson20
