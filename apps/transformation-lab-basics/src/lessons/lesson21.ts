import type { Lesson } from '../engine/types'
import { snapshotRan, modelShown, fileMatches } from '../engine/validators'
import { RAW_CUSTOMERS_CSV } from './_canonical'

const SOURCES_YML = `version: 2

sources:
  - name: raw
    tables:
      - name: customers
`

const SNAP_USER_STATUSES = `{% snapshot snap_user_statuses %}

{{
    config(
      target_schema='main',
      unique_key='id',
      strategy='check',
      check_cols=['country'],
    )
}}

select
    id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}

{% endsnapshot %}
`

const lesson21: Lesson = {
  id: 21,
  title: 'Snapshot Check Strategy',
  concept: `What if your upstream source table does **not** have an \`updated_at\` column?

Many third-party APIs, legacy CRM systems, and Google Sheets don't track update timestamps. If a customer moves from Canada to Germany, the \`country\` column simply changes with no timestamp recorded.

For these tables, dbt provides the **\`check\`** strategy:

\`\`\`sql
{{
    config(
      target_schema='main',
      unique_key='id',
      strategy='check',
      check_cols=['country', 'email'],
    )
}}
\`\`\`

Instead of looking for a timestamp, dbt hashes and compares the values of the columns specified in \`check_cols\`:
- If any checked column value changes, dbt expires the old record (\`dbt_valid_to = current_timestamp\`) and inserts a new active row.
- If the values are unchanged, dbt does nothing.
- You can also pass \`check_cols='all'\` to monitor every column in the SELECT statement.

In this lab, configure \`snap_user_statuses\` to monitor changes to the \`country\` column using the \`check\` strategy.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'snapshots/snap_user_statuses.sql': SNAP_USER_STATUSES,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  tasks: [
    {
      id: 'check_strategy',
      prompt: "Configure `strategy='check'` and `check_cols=['country']` inside `snapshots/snap_user_statuses.sql`.",
      hint: "Make sure `config(...)` has `strategy='check'` and `check_cols=['country']`.",
      validate: (s) =>
        fileMatches(s, 'snapshots/snap_user_statuses.sql', /strategy\s*=\s*['"]check['"]/i) &&
        fileMatches(s, 'snapshots/snap_user_statuses.sql', /check_cols\s*=\s*\[.*country.*\]/i),
    },
    {
      id: 'snapshot',
      prompt: 'Run `dbt snapshot` to build the check-based snapshot in your database.',
      hint: 'Type `dbt snapshot` and press Enter.',
      validate: (s) => snapshotRan(s, 'snap_user_statuses') || s.lastRun?.command === 'snapshot',
    },
    {
      id: 'show',
      prompt: 'Preview the snapshot table with `dbt show --select snap_user_statuses`.',
      hint: 'Run `dbt show --select snap_user_statuses`.',
      validate: (s) => modelShown(s, 'snap_user_statuses'),
    },
  ],
  quiz: {
    question: "What is the primary difference between strategy='timestamp' and strategy='check'?",
    options: [
      "`check` can only be used on views, while `timestamp` only works on tables",
      "`timestamp` relies on an updated_at column, while `check` monitors specific columns for value changes",
      "`check` deletes old rows when a change occurs",
      "`timestamp` requires external plugins",
    ],
    correctIndex: 1,
    explanation: '`strategy="check"` compares the actual values in `check_cols` to identify changes when no reliable `updated_at` timestamp exists.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'snap_user_statuses', label: 'snap_user_statuses', layer: 'intermediate' },
      ],
      edges: [
        { source: 'raw.customers', target: 'snap_user_statuses' },
      ],
    },
  },
  furtherReading: [
    { label: 'Check snapshot strategy', url: 'https://docs.getdbt.com/docs/build/snapshots#check-strategy' },
    { label: 'check_cols configuration', url: 'https://docs.getdbt.com/reference/snapshot-configs#check_cols' },
  ],
}

export default lesson21
