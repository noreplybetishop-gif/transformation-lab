import type { Lesson } from '../engine/types'
import { modelSqlMatches, usedFullRefresh, modelShown } from '../engine/validators'
import { RAW_APP_EVENTS_CSV } from './_canonical'

const SOURCES_YML = `version: 2

sources:
  - name: raw
    tables:
      - name: app_events
`

const STG_APP_EVENTS = `select
    event_id,
    user_id,
    event_name,
    event_timestamp
from {{ source('raw', 'app_events') }}`

const FCT_APP_EVENTS = `{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

select
    event_id,
    user_id,
    event_name,
    event_timestamp
from {{ ref('stg_app_events') }}

{% if is_incremental() %}
  where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}
`

const lesson18: Lesson = {
  id: 18,
  title: 'Full-Refresh Rebuilds',
  concept: `Incremental models are fast because they ignore historical data. But what happens when you:
- Add a new calculated column to your model?
- Modify SQL logic or fix a calculation bug?
- Backfill historical data in your source warehouse?

During regular runs, dbt's \`is_incremental()\` block only processes rows with timestamps after \`max(event_timestamp)\`. As a result, historical rows will **not** receive your new column or updated logic!

To force dbt to rebuild the entire table from scratch, pass the **\`--full-refresh\`** flag:

\`\`\`bash
dbt run --full-refresh --select fct_app_events
\`\`\`

When \`--full-refresh\` is passed:
1. dbt treats \`is_incremental()\` as \`false\`.
2. It drops the existing table and re-executes the query across all history.
3. Your new columns and updated calculations populate across every past row.

In this lab, add an \`is_checkout\` boolean flag to \`fct_app_events\` and execute a full refresh.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/stg_app_events.sql': STG_APP_EVENTS,
    'models/marts/fct_app_events.sql': FCT_APP_EVENTS,
  },
  seeds: {
    'raw.app_events': RAW_APP_EVENTS_CSV,
  },
  preRanModels: ['fct_app_events'],
  tasks: [
    {
      id: 'add_column',
      prompt: "Add a boolean column `event_name = 'checkout' as is_checkout` to the SELECT list in `models/marts/fct_app_events.sql`.",
      hint: "Add `event_name = 'checkout' as is_checkout,` to the columns in `fct_app_events.sql`.",
      validate: (s) =>
        modelSqlMatches(s, 'fct_app_events', /is_checkout/i),
    },
    {
      id: 'full_refresh',
      prompt: 'Execute a full-refresh rebuild by running `dbt run --full-refresh --select fct_app_events`.',
      hint: 'Type `dbt run --full-refresh --select fct_app_events` and press Enter.',
      validate: (s) => usedFullRefresh(s),
    },
    {
      id: 'show',
      prompt: 'Preview the rebuilt table with `dbt show --select fct_app_events` to verify `is_checkout` appears across all rows.',
      hint: 'Run `dbt show --select fct_app_events`.',
      validate: (s) => modelShown(s, 'fct_app_events'),
    },
  ],
  quiz: {
    question: 'When should an engineer run dbt with --full-refresh?',
    options: [
      'On every scheduled cron job in production',
      'Whenever model SQL logic changes or historical records need a full recalculation',
      'Only when dropping the database',
      'To convert a table into a seed',
    ],
    correctIndex: 1,
    explanation: '`--full-refresh` is used intentionally during deployments or data migrations when model schemas or business logic change, ensuring historical rows reflect the latest query definition.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.app_events', label: 'raw.app_events', layer: 'source' },
        { id: 'stg_app_events', label: 'stg_app_events', layer: 'staging' },
        { id: 'fct_app_events', label: 'fct_app_events', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.app_events', target: 'stg_app_events' },
        { source: 'stg_app_events', target: 'fct_app_events' },
      ],
    },
  },
  furtherReading: [
    { label: 'When to use --full-refresh', url: 'https://docs.getdbt.com/docs/build/incremental-models#how-do-i-rebuild-an-incremental-model' },
    { label: 'Full refresh command flag', url: 'https://docs.getdbt.com/reference/commands/run#full-refresh' },
  ],
}

export default lesson18
