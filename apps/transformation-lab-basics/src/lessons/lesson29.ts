import type { Lesson } from '../engine/types'
import { fileMatches, modelSqlMatches, modelRan } from '../engine/validators'
import { RAW_ORDERS_CSV } from './_canonical'

const SOURCES_YML = `version: 2

sources:
  - name: raw
    tables:
      - name: orders
`

const STG_ORDERS = `select
    id as order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ source('raw', 'orders') }}`

const FCT_RECENT_ORDERS = `select
    order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ ref('stg_orders') }}
where created_at >= '{{ var("start_date") }}'
`

const DBT_PROJECT_YML = `name: transformation_lab
version: "1.0.0"
config-version: 2
profile: transformation_lab

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

vars:
  start_date: '2024-01-01'
`

const lesson29: Lesson = {
  id: 29,
  title: 'Project Variables & CLI Overrides',
  concept: `Data pipelines frequently need configurable parameters:
- Filtering date ranges (e.g. processing only records from the last 7 days during development).
- Enabling or disabling experimental transformation features.
- Running historical backfills for a specific partition.

Instead of hardcoding values into SQL files, dbt provides **Variables**.

You declare variables in **\`dbt_project.yml\`**:

\`\`\`yaml
vars:
  start_date: '2024-01-01'
\`\`\`

In your models, you reference them using the **\`var()\`** function:

\`\`\`sql
where created_at >= '{{ var("start_date") }}'
\`\`\`

You can also provide a default fallback if the variable isn't defined: \`var("start_date", "2024-01-01")\`.

Crucially, you can override any variable at runtime from the CLI using the **\`--vars\`** argument without modifying any repository files:

\`\`\`bash
dbt run --select fct_recent_orders --vars '{"start_date": "2024-02-01"}'
\`\`\`

In this lab, inspect \`dbt_project.yml\`, review \`fct_recent_orders.sql\`, and execute dbt with a runtime \`--vars\` override.`,
  initialFiles: {
    'dbt_project.yml': DBT_PROJECT_YML,
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/stg_orders.sql': STG_ORDERS,
    'models/marts/fct_recent_orders.sql': FCT_RECENT_ORDERS,
  },
  seeds: {
    'raw.orders': RAW_ORDERS_CSV,
  },
  openFiles: ['dbt_project.yml', 'models/marts/fct_recent_orders.sql'],
  tasks: [
    {
      id: 'var_in_project',
      prompt: "Verify `dbt_project.yml` declares `vars:` with `start_date: '2024-01-01'`.",
      hint: "Check that `dbt_project.yml` includes `vars: start_date: '2024-01-01'`.",
      validate: (s) =>
        fileMatches(s, 'dbt_project.yml', /vars:\s*\n\s*start_date:/i),
    },
    {
      id: 'var_in_sql',
      prompt: "Ensure `models/marts/fct_recent_orders.sql` filters with `where created_at >= '{{ var(\"start_date\") }}'`.",
      hint: "Make sure `fct_recent_orders.sql` calls `var('start_date')` in the WHERE clause.",
      validate: (s) =>
        modelSqlMatches(s, 'fct_recent_orders', /var\s*\(\s*['"]start_date['"]\s*\)/i),
    },
    {
      id: 'run_with_vars',
      prompt: 'Run `dbt run --select fct_recent_orders --vars \'{"start_date": "2024-02-01"}\'` in the terminal.',
      hint: "Type: dbt run --select fct_recent_orders --vars '{\"start_date\": \"2024-02-01\"}'",
      validate: (s) => modelRan(s, 'fct_recent_orders'),
    },
  ],
  quiz: {
    question: 'How do you pass variable overrides to dbt from the command line?',
    options: [
      'Using the --env flag with comma-separated values',
      'Using the --vars flag with a JSON string (e.g. --vars \'{"key": "value"}\')',
      'By modifying the .env file before running',
      'Using the --config flag with YAML syntax',
    ],
    correctIndex: 1,
    explanation: 'The `--vars` flag accepts a YAML or JSON string dictionary from the command line, overriding any variables configured in `dbt_project.yml`.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.orders', label: 'raw.orders', layer: 'source' },
        { id: 'stg_orders', label: 'stg_orders', layer: 'staging' },
        { id: 'fct_recent_orders', label: 'fct_recent_orders', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.orders', target: 'stg_orders' },
        { source: 'stg_orders', target: 'fct_recent_orders' },
      ],
    },
  },
  furtherReading: [
    { label: 'Using project variables (vars)', url: 'https://docs.getdbt.com/docs/build/project-variables' },
    { label: 'var function reference', url: 'https://docs.getdbt.com/reference/dbt-jinja-functions/var' },
  ],
}

export default lesson29
