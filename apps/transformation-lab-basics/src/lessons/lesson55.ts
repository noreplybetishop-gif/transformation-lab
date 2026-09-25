import type { Lesson } from '../engine/types'
import { commandRan, fileContains } from '../engine/validators'
import { RAW_CUSTOMERS_CSV } from './_canonical'

const DBT_PROJECT_YML = `name: transformation_lab
version: "1.0.0"
config-version: 2
profile: transformation_lab

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
macro-paths: ["macros"]

on-run-start:
  - "-- Run start hook: initialize session state or log startup"

on-run-end:
  - "-- Run end hook: log invocation result: {{ results | length }} models processed"

models:
  transformation_lab:
    staging:
      +materialized: view
`

const STG_CUSTOMERS = `select
    id as customer_id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}
`

const lesson55: Lesson = {
  id: 55,
  title: 'On-Run-Start & On-Run-End Hooks',
  concept: `Unlike pre-hooks and post-hooks that run once *per model*, **\`on-run-start\`** and **\`on-run-end\`** hooks execute exactly once per invocation of the entire dbt command (e.g. at the very start and end of \`dbt build\`).

They are declared in \`dbt_project.yml\`:
\`\`\`yaml
on-run-start:
  - "create table if not exists audit.runs (run_id varchar, started_at timestamp)"
  - "insert into audit.runs values ('{{ invocation_id }}', current_timestamp)"

on-run-end:
  - "{% if results %} -- Inspect execution outcomes {% endif %}"
\`\`\`
Inside \`on-run-end\`, you have access to the **\`results\`** context variable! This list contains every executed model's status (\`success\`, \`error\`, \`skipped\`), execution duration, and message - allowing you to write automated Slack webhook alerts or database run summaries directly from dbt.`,
  initialFiles: {
    'dbt_project.yml': DBT_PROJECT_YML,
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: customers\n`,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  tasks: [
    {
      id: 'verify_lifecycle_hooks',
      prompt: "Inspect `dbt_project.yml` to verify that `on-run-start:` and `on-run-end:` are configured.",
      hint: "Check that on-run-start and on-run-end are present in dbt_project.yml.",
      validate: (s) =>
        fileContains(s, 'dbt_project.yml', 'on-run-start') &&
        fileContains(s, 'dbt_project.yml', 'on-run-end'),
    },
    {
      id: 'run_build',
      prompt: 'Execute `dbt build --select stg_customers` to trigger the lifecycle hooks.',
      hint: 'Type `dbt build --select stg_customers` in the console.',
      validate: (s) => commandRan(s, 'build') || commandRan(s, 'run'),
    },
  ],
  quiz: {
    question: 'What special context variable is accessible inside on-run-end hooks to check the status of completed models?',
    options: [
      'results',
      'table_stats',
      'error_code',
      'schema_cache',
    ],
    correctIndex: 0,
    explanation: 'The results variable in on-run-end holds a list of RunResult objects detailing execution status, node IDs, timing, and error messages for everything dbt executed.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'stg_customers', label: 'stg_customers', layer: 'staging' },
      ],
      edges: [
        { source: 'raw.customers', target: 'stg_customers' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt on-run-start & on-run-end', url: 'https://docs.getdbt.com/reference/project-configs/on-run-start-on-run-end' },
  ],
}

export default lesson55
