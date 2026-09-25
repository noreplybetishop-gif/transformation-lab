import type { Lesson } from '../engine/types'
import { modelRan, modelSqlMatches, fileContains } from '../engine/validators'
import { RAW_APP_EVENTS_CSV } from './_canonical'

const STG_APP_EVENTS = `select
    event_id,
    user_id,
    event_name,
    event_timestamp
from {{ source('raw', 'app_events') }}
`

const lesson36: Lesson = {
  id: 36,
  title: 'Append-Only Incremental Strategy',
  concept: `For high-velocity datasets like raw clickstreams, IoT telemetry, and audit logs, records are strictly immutable: once created, an event is never updated.

Running a \`merge\` join across billions of immutable rows wastes immense CPU and I/O comparing keys. Instead, dbt offers:
\`\`\`sql
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}
\`\`\`
With \`strategy='append'\`, dbt executes a raw \`INSERT INTO destination ...\` with zero join or deduplication overhead. You filter for new records using \`is_incremental()\` and a timestamp watermark:

\`\`\`sql
{% if is_incremental() %}
  where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}
\`\`\``,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: app_events\n`,
    'models/staging/stg_app_events.sql': STG_APP_EVENTS,
    'models/marts/fct_event_logs_append.sql': `-- Configure append incremental strategy\n\nselect\n    event_id,\n    user_id,\n    event_name,\n    event_timestamp\nfrom {{ ref('stg_app_events') }}\n`,
  },
  seeds: {
    'raw.app_events': RAW_APP_EVENTS_CSV,
  },
  tasks: [
    {
      id: 'configure_append',
      prompt: "Configure `fct_event_logs_append.sql` with `materialized='incremental'` and `incremental_strategy='append'`.",
      hint: "Add {{ config(materialized='incremental', incremental_strategy='append') }} at the top of the model.",
      validate: (s) =>
        fileContains(s, 'models/marts/fct_event_logs_append.sql', 'incremental_strategy') &&
        modelSqlMatches(s, 'fct_event_logs_append', /incremental_strategy\s*=\s*['"]append['"]/),
    },
    {
      id: 'run_model',
      prompt: 'Execute `dbt run --select fct_event_logs_append` to run the append-only pipeline.',
      hint: 'Type `dbt run --select fct_event_logs_append` in the console.',
      validate: (s) => modelRan(s, 'fct_event_logs_append'),
    },
  ],
  quiz: {
    question: 'Why is incremental_strategy="append" significantly faster than "merge" for event streams?',
    options: [
      'It skips building the table in the warehouse',
      'It avoids scanning the target table for matching unique keys and directly executes an INSERT',
      'It compresses data using gzip automatically',
      'It runs tests before inserting any rows',
    ],
    correctIndex: 1,
    explanation: 'Merge requires scanning existing target table keys to decide between UPDATE and INSERT. Append-only knows records are purely additions, writing new rows directly with zero key-comparison overhead.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.app_events', label: 'raw.app_events', layer: 'source' },
        { id: 'stg_app_events', label: 'stg_app_events', layer: 'staging' },
        { id: 'fct_event_logs_append', label: 'fct_event_logs_append', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.app_events', target: 'stg_app_events' },
        { source: 'stg_app_events', target: 'fct_event_logs_append' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt append strategy', url: 'https://docs.getdbt.com/docs/build/incremental-strategy#append' },
  ],
}

export default lesson36
