import type { Lesson } from '../engine/types'
import { fileMatches, buildSucceeded, modelShown } from '../engine/validators'
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

const MARTS_SCHEMA_YML = `version: 2

models:
  - name: dim_customers
    description: "Core dimensional model for verified customer accounts."
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: integer
        data_tests:
          - not_null
          - unique
      - name: name
        data_type: varchar
      - name: email
        data_type: varchar
      - name: country
        data_type: varchar
`

const lesson31: Lesson = {
  id: 31,
  title: 'Model Contracts & Schema Enforcement',
  concept: `In software engineering, API contracts guarantee that endpoints don't break downstream services.

In analytics engineering, **Model Contracts** provide that exact guarantee for your data models:
\`\`\`yaml
config:
  contract:
    enforced: true
\`\`\`

Why do enterprise data teams rely on model contracts?
1. **Guaranteed Column Sets**: If a developer accidentally deletes or renames \`customer_id\` in the SQL query, dbt fails immediately during compilation instead of breaking downstream Looker or Tableau dashboards.
2. **Strict Type Safety**: If a column unexpectedly changes from \`integer\` to \`varchar\`, the build aborts before bad types pollute your warehouse.
3. **Producer-Consumer Trust**: Upstream data producers can publish breaking-change-free guarantees to downstream analytics consumers.

When \`contract.enforced: true\` is set, dbt requires:
- Every column output by the query must be declared in the YAML schema.
- Every declared column must define a matching \`data_type\`.

In this lab, inspect the contract configuration on \`dim_customers\` in \`_schema.yml\` and run \`dbt build\`.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
    'models/marts/dim_customers.sql': DIM_CUSTOMERS,
    'models/marts/_schema.yml': MARTS_SCHEMA_YML,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  openFiles: ['models/marts/_schema.yml', 'models/marts/dim_customers.sql'],
  tasks: [
    {
      id: 'verify_contract',
      prompt: "Ensure `models/marts/_schema.yml` declares `config: contract: enforced: true` under `dim_customers`.",
      hint: "Check `models/marts/_schema.yml` for `enforced: true` and explicit `data_type` definitions.",
      validate: (s) =>
        fileMatches(s, 'models/marts/_schema.yml', /contract:\s*\n\s*enforced:\s*true/i) &&
        fileMatches(s, 'models/marts/_schema.yml', /data_type:\s*integer/i),
    },
    {
      id: 'build_contract',
      prompt: 'Run `dbt build --select dim_customers` to validate that the SQL matches the contract.',
      hint: 'Run `dbt build --select dim_customers` in the terminal.',
      validate: (s) => buildSucceeded(s),
    },
    {
      id: 'show_contract',
      prompt: 'Preview the contracted model with `dbt show --select dim_customers`.',
      hint: 'Type `dbt show --select dim_customers`.',
      validate: (s) => modelShown(s, 'dim_customers'),
    },
  ],
  quiz: {
    question: 'What happens if a model with contract.enforced: true produces a column not listed in its YAML schema?',
    options: [
      'dbt silently discards the extra column',
      'dbt throws a contract error during model compilation and refuses to build the table',
      'dbt adds the column as a comment',
      'dbt converts the model to a seed',
    ],
    correctIndex: 1,
    explanation: 'With model contracts enforced, any mismatch between the query output columns and the YAML column contract (including unexpected or missing columns) immediately fails the build.',
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
    { label: 'dbt Model Contracts', url: 'https://docs.getdbt.com/docs/collaborate/govern/model-contracts' },
    { label: 'Column constraints & types', url: 'https://docs.getdbt.com/reference/resource-properties/constraints' },
  ],
}

export default lesson31
