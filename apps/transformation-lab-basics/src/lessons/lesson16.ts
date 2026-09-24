import type { Lesson } from '../engine/types'
import { modelSqlMatches, modelCompiled, modelRan } from '../engine/validators'
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

const FCT_APP_EVENTS_INIT = `{{ config(materialized='incremental') }}

select
    event_id,
    user_id,
    event_name,
    event_timestamp
from {{ ref('stg_app_events') }}
`

const lesson16: Lesson = {
  id: 16,
  title: 'The is_incremental() Macro & Watermarks',
  concept: `Declaring \`materialized='incremental'\` is only half the battle. Without a filter, your model query would still scan every single upstream row on every execution!

To filter only the newest records, dbt provides the **\`is_incremental()\`** macro. It returns:
- \`false\` on the first run (or when running with \`--full-refresh\`).
- \`true\` during normal subsequent runs.

You wrap your watermark filter in a Jinja conditional:

\`\`\`sql
{% if is_incremental() %}
  where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}
\`\`\`

The \`{{ this }}\` keyword is special: dbt resolves it to the **destination table itself** in your database (\`transformation_lab.main.fct_app_events\`).

During incremental runs, the subquery checks the highest timestamp already stored in the destination table, so only strictly newer events are processed.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/stg_app_events.sql': STG_APP_EVENTS,
    'models/marts/fct_app_events.sql': FCT_APP_EVENTS_INIT,
  },
  seeds: {
    'raw.app_events': RAW_APP_EVENTS_CSV,
  },
  preRanModels: ['fct_app_events'],
  tasks: [
    {
      id: 'watermark',
      prompt: "Add the `{% if is_incremental() %}` filter to `models/marts/fct_app_events.sql` filtering on `event_timestamp > (select max(event_timestamp) from {{ this }})`.",
      hint: "Add `{% if is_incremental() %} where event_timestamp > (select max(event_timestamp) from {{ this }}) {% endif %}` at the end of the query.",
      validate: (s) =>
        modelSqlMatches(s, 'fct_app_events', /is_incremental\s*\(\s*\)/i) &&
        modelSqlMatches(s, 'fct_app_events', /max\s*\(\s*event_timestamp\s*\)\s*from\s*\{\{\s*this\s*\}\}/i),
    },
    {
      id: 'compile',
      prompt: 'Run `dbt compile --select fct_app_events` to inspect how dbt compiles the {{ this }} reference.',
      hint: 'Run `dbt compile --select fct_app_events` in the terminal.',
      validate: (s) => modelCompiled(s, 'fct_app_events'),
    },
    {
      id: 'run',
      prompt: 'Run `dbt run --select fct_app_events` to execute the incremental build against the warehouse.',
      hint: 'Type `dbt run --select fct_app_events` and press Enter.',
      validate: (s) => modelRan(s, 'fct_app_events'),
    },
  ],
  quiz: {
    question: 'What does the {{ this }} keyword represent inside a dbt model?',
    options: [
      'The source table being read from',
      'The current model relation (target table in the database)',
      'The active git branch',
      'The dbt project directory path',
    ],
    correctIndex: 1,
    explanation: '`{{ this }}` is an internal dbt relation object representing the existing database table that the model writes into.',
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
    { label: 'Using is_incremental()', url: 'https://docs.getdbt.com/docs/build/incremental-models#how-do-i-use-the-is_incremental-macro' },
    { label: 'The this variable', url: 'https://docs.getdbt.com/reference/dbt-jinja-functions/this' },
  ],
}

export default lesson16
