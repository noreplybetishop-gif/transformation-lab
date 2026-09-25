import type { Lesson } from '../engine/types'
import { fileContains, commandRan } from '../engine/validators'
import { RAW_ORDERS_CSV } from './_canonical'

const SOURCES_YML = `version: 2

sources:
  - name: raw
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
    loaded_at_field: created_at
    tables:
      - name: orders
`

const lesson45: Lesson = {
  id: 45,
  title: 'Source Freshness & Data SLAs',
  concept: `Transforming stale data leads to stale business decisions. If an Fivetran, Airbyte, or Kafka ingestion pipeline silently halts, dbt transformations will continue to build cleanly on old data without warning.

dbt solves this with **Source Freshness**:
\`\`\`yaml
sources:
  - name: raw
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
    loaded_at_field: created_at
    tables:
      - name: orders
\`\`\`
Running \`dbt source freshness\` inspects the \`MAX(created_at)\` of your raw source tables against current system time. If the ingestion pipeline is delayed beyond your SLA window, dbt flags the source before any downstream models run!`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/stg_orders.sql': `select * from {{ source('raw', 'orders') }}\n`,
  },
  seeds: {
    'raw.orders': RAW_ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_freshness_config',
      prompt: "Inspect `models/staging/_sources.yml` to verify that `freshness:` and `loaded_at_field:` are defined on `raw`.",
      hint: "Check that warn_after, error_after, and loaded_at_field are present.",
      validate: (s) =>
        fileContains(s, 'models/staging/_sources.yml', 'freshness') &&
        fileContains(s, 'models/staging/_sources.yml', 'loaded_at_field'),
    },
    {
      id: 'compile_sources',
      prompt: 'Execute `dbt compile --select source:raw.orders` to validate the source graph and schema metadata.',
      hint: 'Run `dbt compile --select source:raw.orders`.',
      validate: (s) => commandRan(s, 'compile') || commandRan(s, 'build'),
    },
  ],
  quiz: {
    question: 'What does dbt source freshness query to determine whether source data meets SLA requirements?',
    options: [
      'The number of rows in the table',
      'The maximum value of the configured loaded_at_field timestamp compared to the current timestamp',
      'The git commit date of the YAML file',
      'The file size of the source CSV',
    ],
    correctIndex: 1,
    explanation: 'dbt runs SELECT MAX(loaded_at_field) FROM source_table and compares the elapsed time against the warn_after and error_after intervals.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.orders', label: 'raw.orders', layer: 'source' },
        { id: 'stg_orders', label: 'stg_orders', layer: 'staging' },
      ],
      edges: [
        { source: 'raw.orders', target: 'stg_orders' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Source Freshness', url: 'https://docs.getdbt.com/docs/build/sources#source-data-freshness' },
  ],
}

export default lesson45
