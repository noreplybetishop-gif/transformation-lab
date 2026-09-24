import type { Lesson } from '../engine/types'
import { fileMatches, modelRan, modelShown } from '../engine/validators'
import { RAW_CUSTOMERS_CSV } from './_canonical'

const SOURCES_YML = `version: 2

sources:
  - name: raw
    tables:
      - name: customers
`

const STG_CUSTOMERS = `select
    id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}
`

const DIM_CUSTOMERS = `select
    id as customer_id,
    name,
    email,
    country
from {{ ref('stg_customers') }}
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

models:
  transformation_lab:
    staging:
      +materialized: view
    marts:
      +materialized: table
      +schema: marts
`

const lesson30: Lesson = {
  id: 30,
  title: 'Custom Schemas & Multi-Schema Architecture',
  concept: `In a production database, you should never dump all models into a single default schema.

Production architectures separate data access layers across dedicated schemas:
- **\`staging\`**: Cleaned, standardized mirrors of raw sources.
- **\`intermediate\`**: Complex joins and business aggregations (internal only).
- **\`marts\` / \`analytics\`**: Public-facing dimensional models consumed by BI dashboards and reporting tools.

In dbt, you assign custom schemas to models using the **\`+schema\`** configuration in \`dbt_project.yml\` (or inside individual \`config()\` blocks):

\`\`\`yaml
models:
  transformation_lab:
    staging:
      +materialized: view
    marts:
      +materialized: table
      +schema: marts
\`\`\`

By default, dbt concatenates the target schema with the custom schema: \`<target_schema>_<custom_schema>\` (e.g. \`main_marts\`). This prevents developers from accidentally overwriting production tables when developing in dev environments.

In this lab, inspect the \`+schema: marts\` setting in \`dbt_project.yml\`, build the marts, and verify where dbt routes the table.`,
  initialFiles: {
    'dbt_project.yml': DBT_PROJECT_YML,
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
    'models/marts/dim_customers.sql': DIM_CUSTOMERS,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  openFiles: ['dbt_project.yml', 'models/marts/dim_customers.sql'],
  tasks: [
    {
      id: 'verify_schema_config',
      prompt: "Ensure `dbt_project.yml` sets `+schema: marts` for all models under `marts:`.",
      hint: "Check `dbt_project.yml` under `marts:` for `+schema: marts`.",
      validate: (s) =>
        fileMatches(s, 'dbt_project.yml', /\+schema:\s*marts/i),
    },
    {
      id: 'run_marts',
      prompt: 'Run `dbt run --select marts` to materialize the models into the custom marts schema.',
      hint: 'Run `dbt run --select marts`.',
      validate: (s) => modelRan(s, 'dim_customers'),
    },
    {
      id: 'show_dim',
      prompt: 'Preview the materialized table with `dbt show --select dim_customers`.',
      hint: 'Run `dbt show --select dim_customers`.',
      validate: (s) => modelShown(s, 'dim_customers'),
    },
  ],
  quiz: {
    question: 'How does dbt name schemas by default when a custom schema is configured in a dev environment?',
    options: [
      'It discards the dev schema and writes to the custom schema directly',
      'It concatenates them: <target_schema>_<custom_schema>',
      'It creates a temporary random schema name',
      'It fails unless generate_schema_name is installed',
    ],
    correctIndex: 1,
    explanation: 'By default, dbt prepends the active target schema name (e.g. `dbt_alice_marts` or `main_marts`) so developers working in dev do not overwrite production schemas.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'stg_customers', label: 'stg_customers', layer: 'staging' },
        { id: 'dim_customers', label: 'dim_customers', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.customers', target: 'stg_customers' },
        { source: 'stg_customers', target: 'dim_customers' },
      ],
    },
  },
  furtherReading: [
    { label: 'Using custom schemas', url: 'https://docs.getdbt.com/docs/build/custom-schemas' },
    { label: 'generate_schema_name macro', url: 'https://docs.getdbt.com/docs/build/custom-schemas#how-does-dbt-generate-a-schemas-name' },
  ],
}

export default lesson30
