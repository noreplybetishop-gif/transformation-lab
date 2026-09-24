import type { Lesson } from '../engine/types'
import { modelMaterialization, modelRan, modelShown } from '../engine/validators'
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

const FCT_APP_EVENTS = `select
    event_id,
    user_id,
    event_name,
    event_timestamp
from {{ ref('stg_app_events') }}`

const lesson15: Lesson = {
  id: 15,
  title: 'Introduction to Incremental Models',
  concept: `In the Basics course, every model was materialized as either a **view** (recomputed on every query) or a **table** (dropped and recreated from scratch with \`CREATE TABLE AS SELECT\`).

For high-volume tables - like web events, payment streams, or IoT telemetry - rebuilding millions of rows from scratch on every \`dbt run\` is slow, expensive, and wastes compute.

**Incremental models** solve this by transforming and inserting only *new or updated* rows into the destination table:

- On the **first run**, dbt creates the table containing all historical data.
- On **subsequent runs**, dbt queries only rows newer than what already exists in the table.

You set a model to build incrementally with the materialization config:

\`\`\`sql
{{ config(materialized='incremental') }}
\`\`\`

In this lab, you have an incoming stream of clickstream events in \`raw.app_events\`. Configure \`fct_app_events\` as incremental and run it.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/stg_app_events.sql': STG_APP_EVENTS,
    'models/marts/fct_app_events.sql': FCT_APP_EVENTS,
  },
  seeds: {
    'raw.app_events': RAW_APP_EVENTS_CSV,
  },
  tasks: [
    {
      id: 'config',
      prompt: "Configure `models/marts/fct_app_events.sql` to materialize as incremental using `{{ config(materialized='incremental') }}`.",
      hint: "Add `{{ config(materialized='incremental') }}` at the very top of `models/marts/fct_app_events.sql`.",
      validate: (s) => modelMaterialization(s, 'fct_app_events', 'incremental'),
    },
    {
      id: 'run',
      prompt: 'Run `dbt run --select fct_app_events` to build the model in your database.',
      hint: 'Run `dbt run --select fct_app_events` in the terminal.',
      validate: (s) => modelRan(s, 'fct_app_events'),
    },
    {
      id: 'show',
      prompt: 'Preview the materialized table rows using `dbt show --select fct_app_events`.',
      hint: 'Type `dbt show --select fct_app_events` and press Enter.',
      validate: (s) => modelShown(s, 'fct_app_events'),
    },
  ],
  quiz: {
    question: 'How does dbt handle the very first run of an incremental model?',
    options: [
      'It creates an empty table and awaits incremental appends',
      'It creates the full table with all rows, equivalent to table materialization',
      'It fails if the destination table does not already exist in the database',
      'It builds a view instead of a table',
    ],
    correctIndex: 1,
    explanation: 'On the initial run, the destination table does not exist yet. dbt automatically executes a full table build. All subsequent runs apply incremental logic.',
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
    { label: 'Incremental models documentation', url: 'https://docs.getdbt.com/docs/build/incremental-models' },
    { label: 'Configuring materializations', url: 'https://docs.getdbt.com/reference/model-configs/materialized' },
  ],
}

export default lesson15
