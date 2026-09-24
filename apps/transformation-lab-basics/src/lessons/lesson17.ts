import type { Lesson } from '../engine/types'
import { modelSqlMatches, testDefinitionsInclude, buildSucceeded } from '../engine/validators'
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
    materialized='incremental'
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

const MARTS_SCHEMA_YML = `version: 2

models:
  - name: fct_app_events
    description: "High-volume user event stream with incremental deduplication."
    columns:
      - name: event_id
        description: "Primary key for each event."
`

const lesson17: Lesson = {
  id: 17,
  title: 'Unique Keys & Deduplication',
  concept: `By default, an incremental model simply appends new records (\`INSERT INTO\`). But real-world data is messy:
- Upstream systems re-emit past events when retrying failures.
- Late-arriving records overlap with previous runs.
- Existing records are updated with new statuses.

Without a deduplication strategy, re-running your pipeline creates duplicate records.

To prevent this, define a **\`unique_key\`** in your model configuration:

\`\`\`sql
{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}
\`\`\`

When a \`unique_key\` is specified, dbt uses a **merge** strategy (or delete + insert) under the hood:
- If a row with that \`event_id\` already exists in the destination table, dbt **updates** it.
- If the \`event_id\` is new, dbt **inserts** it.

You can also pass a list for composite primary keys: \`unique_key=['user_id', 'event_id']\`.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/stg_app_events.sql': STG_APP_EVENTS,
    'models/marts/fct_app_events.sql': FCT_APP_EVENTS,
    'models/marts/_schema.yml': MARTS_SCHEMA_YML,
  },
  seeds: {
    'raw.app_events': RAW_APP_EVENTS_CSV,
  },
  preRanModels: ['fct_app_events'],
  tasks: [
    {
      id: 'unique_key',
      prompt: "Configure `unique_key='event_id'` inside the `config()` block of `models/marts/fct_app_events.sql`.",
      hint: "Update `config()` to: `{{ config(materialized='incremental', unique_key='event_id') }}`.",
      validate: (s) =>
        modelSqlMatches(s, 'fct_app_events', /unique_key\s*=\s*['"]event_id['"]/i),
    },
    {
      id: 'tests',
      prompt: "Add `unique` and `not_null` data tests to `event_id` in `models/marts/_schema.yml`.",
      hint: "Under `event_id`, add `data_tests:` with `- unique` and `- not_null`.",
      validate: (s) => testDefinitionsInclude(s, 'fct_app_events', ['unique', 'not_null']),
    },
    {
      id: 'build',
      prompt: 'Run `dbt build --select fct_app_events` to build the model and verify the tests pass.',
      hint: 'Run `dbt build --select fct_app_events` in the terminal.',
      validate: (s) => buildSucceeded(s),
    },
  ],
  quiz: {
    question: 'What happens when an incremental model runs with a unique_key and encounters an existing ID?',
    options: [
      'dbt raises a DuplicateKeyError and aborts',
      'dbt updates the existing row with the incoming data instead of creating a duplicate',
      'dbt silently discards the incoming row',
      'dbt converts the model back into a view',
    ],
    correctIndex: 1,
    explanation: 'With a `unique_key`, dbt merges incoming records. Matching existing rows are updated, and new rows are appended.',
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
    { label: 'Defining a unique_key', url: 'https://docs.getdbt.com/docs/build/incremental-models#what-is-a-unique_key' },
    { label: 'Incremental strategies', url: 'https://docs.getdbt.com/docs/build/incremental-strategy' },
  ],
}

export default lesson17
