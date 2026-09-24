import type { Lesson } from '../engine/types'
import { modelSqlMatches, modelRan } from '../engine/validators'
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

const DIM_CUSTOMERS = `{{ config(
    post_hook="-- audit: {{ this }} refreshed at {{ run_started_at }}"
) }}

select
    id as customer_id,
    name,
    email,
    country
from {{ ref('stg_customers') }}
`

const lesson32: Lesson = {
  id: 32,
  title: 'Pre-hooks, Post-hooks & Grants',
  concept: `Data pipelines often need to run auxiliary commands before or after a table is created:
- Granting \`SELECT\` privileges to BI users and analyst roles.
- Inserting an execution log entry into an audit table.
- Creating specialized database indexes or vacuuming storage.

dbt handles these lifecycle events through **Hooks**:

- **\`pre_hook\`**: Runs SQL immediately *before* the model query executes.
- **\`post_hook\`**: Runs SQL immediately *after* the model query completes.

You configure hooks in your model's \`config()\` block or globally in \`dbt_project.yml\`:

\`\`\`sql
{{ config(
    post_hook="-- audit: {{ this }} refreshed at {{ run_started_at }}"
) }}
\`\`\`

Notice the built-in Jinja variables:
- \`{{ this }}\`: Resolves to the destination relation.
- \`{{ run_started_at }}\`: Current pipeline execution timestamp.

For access control, dbt also provides native **Grants** configuration:
\`\`\`yaml
config:
  grants:
    select: ['bi_analysts', 'reporting_role']
\`\`\`
dbt automatically executes the appropriate \`GRANT SELECT\` statements during model creation.

In this lab, inspect the \`post_hook\` configuration in \`dim_customers.sql\` and run \`dbt run\`.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
    'models/marts/dim_customers.sql': DIM_CUSTOMERS,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  tasks: [
    {
      id: 'verify_hook',
      prompt: "Verify `models/marts/dim_customers.sql` contains a `post_hook` in its `config()` block.",
      hint: "Check that `config(post_hook=...)` is present in `dim_customers.sql`.",
      validate: (s) =>
        modelSqlMatches(s, 'dim_customers', /post_hook\s*=/i),
    },
    {
      id: 'run_hook',
      prompt: 'Run `dbt run --select dim_customers` to execute the model and its post-hook.',
      hint: 'Run `dbt run --select dim_customers`.',
      validate: (s) => modelRan(s, 'dim_customers'),
    },
  ],
  quiz: {
    question: 'When does a post_hook execute relative to the model query?',
    options: [
      'Before dbt compiles the SQL files',
      'Immediately after the model finishes materializing in the database',
      'Only if the model query throws an error',
      'At the start of the entire project run',
    ],
    correctIndex: 1,
    explanation: 'A `post_hook` runs SQL statements in the database transaction immediately after the model successfully completes its materialization.',
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
    { label: 'dbt Hooks reference', url: 'https://docs.getdbt.com/reference/resource-configs/pre-hook-post-hook' },
    { label: 'Configuring grants', url: 'https://docs.getdbt.com/reference/resource-configs/grants' },
  ],
}

export default lesson32
