import type { Lesson } from '../engine/types'
import { snapshotRan, modelShown, fileMatches } from '../engine/validators'
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

const lesson19: Lesson = {
  id: 19,
  title: 'Dimension History with Snapshots',
  concept: `Most operational databases overwrite data in-place. When a customer upgrades from \`basic\` to \`pro\`, an update overwrites the old status:

\`\`\`sql
UPDATE subscriptions SET plan = 'pro' WHERE customer_id = 1;
\`\`\`

If an analyst asks *"How many users were on the Basic plan on March 1st?"*, that historical answer is permanently gone!

In analytics engineering, tracking changes over time is called **Slowly Changing Dimensions (SCD Type 2)**.

dbt implements SCD Type 2 natively through **Snapshots**:
- Snapshot definitions live in the \`snapshots/\` folder as \`.sql\` files.
- Wrapped in \`{% snapshot <name> %} ... {% endsnapshot %}\`.
- Ran via the dedicated command: \`dbt snapshot\`.

When you run \`dbt snapshot\`, dbt monitors the source table and automatically appends four metadata columns:
1. **\`dbt_scd_id\`**: A unique primary key for the record version.
2. **\`dbt_updated_at\`**: When the source record was modified.
3. **\`dbt_valid_from\`**: Timestamp when this state became active.
4. **\`dbt_valid_to\`**: Timestamp when this state was superseded (\`NULL\` means it is the current active record).

In this lab, inspect the snapshot definition and execute your first \`dbt snapshot\`.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'snapshots/snap_customer_subscriptions.sql': SNAP_SUBSCRIPTIONS,
  },
  seeds: {
    'raw.subscriptions': RAW_SUBSCRIPTIONS_CSV,
  },
  tasks: [
    {
      id: 'snapshot_file',
      prompt: 'Verify `snapshots/snap_customer_subscriptions.sql` declares a snapshot targeting `raw.subscriptions`.',
      hint: 'The file should use `{% snapshot snap_customer_subscriptions %}` and select from the raw subscriptions source.',
      validate: (s) =>
        fileMatches(s, 'snapshots/snap_customer_subscriptions.sql', /snapshot\s+snap_customer_subscriptions/i) &&
        fileMatches(s, 'snapshots/snap_customer_subscriptions.sql', /endsnapshot/i),
    },
    {
      id: 'run_snapshot',
      prompt: 'Execute `dbt snapshot` in the terminal to initialize the snapshot table.',
      hint: 'Type `dbt snapshot` and press Enter.',
      validate: (s) => snapshotRan(s, 'snap_customer_subscriptions') || s.lastRun?.command === 'snapshot',
    },
    {
      id: 'show_snapshot',
      prompt: 'Preview the snapshot table by running `dbt show --select snap_customer_subscriptions`.',
      hint: 'Run `dbt show --select snap_customer_subscriptions`.',
      validate: (s) => modelShown(s, 'snap_customer_subscriptions'),
    },
  ],
  quiz: {
    question: 'In a dbt snapshot table, what does a NULL value in dbt_valid_to indicate?',
    options: [
      'The row has been deleted from the database',
      'The row is currently active and represents the present state of the entity',
      'The snapshot encountered an error during recording',
      'The row is historical and no longer valid',
    ],
    correctIndex: 1,
    explanation: '`dbt_valid_to IS NULL` signifies the latest, currently active version of a dimension record in SCD Type 2.',
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
    { label: 'dbt Snapshots overview', url: 'https://docs.getdbt.com/docs/build/snapshots' },
    { label: 'Snapshot configurations', url: 'https://docs.getdbt.com/reference/snapshot-configs' },
  ],
}

export default lesson19
