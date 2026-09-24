import type { Lesson } from '../engine/types'
import { fileMatches, modelRan } from '../engine/validators'
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

const DIM_CUSTOMERS = `{{ config(materialized='table') }}

select
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
`

const lesson28: Lesson = {
  id: 28,
  title: 'Folder-Level Configuration',
  concept: `So far, you've configured materializations by adding \`{{ config(materialized='table') }}\` to individual SQL files.

In production repositories with hundreds of models, repeating \`config()\` in every file creates clutter and invites inconsistency. What if your data team agrees that:
- Every **staging** model must be a \`view\`?
- Every **mart** model must be a \`table\`?

In dbt, you configure entire directories hierarchically inside **\`dbt_project.yml\`**:

\`\`\`yaml
models:
  transformation_lab:
    staging:
      +materialized: view
    marts:
      +materialized: table
\`\`\`

Notice the **\`+\` prefix**: in dbt YAML, \`+\` indicates a configuration property (like \`+materialized\`, \`+tags\`, or \`+schema\`).

Any model saved in \`models/marts/\` automatically inherits \`materialized: table\` without needing any inline \`config()\` block. Individual models can still override it if necessary, but the project-level default keeps the repository uniform and clean.

In this lab, inspect \`dbt_project.yml\`, remove the redundant inline config from \`dim_customers.sql\`, and run \`dbt run\`.`,
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
      id: 'verify_project_config',
      prompt: "Ensure `dbt_project.yml` configures `+materialized: table` under `models: transformation_lab: marts:`.",
      hint: "Check `dbt_project.yml` for `marts:` with `+materialized: table`.",
      validate: (s) =>
        fileMatches(s, 'dbt_project.yml', /marts:\s*\n\s*\+materialized:\s*table/i),
    },
    {
      id: 'clean_model',
      prompt: "Remove `{{ config(materialized='table') }}` from `models/marts/dim_customers.sql` since it now inherits this from `dbt_project.yml`.",
      hint: "Delete the `{{ config(...) }}` line from `models/marts/dim_customers.sql`.",
      validate: (s) =>
        !fileMatches(s, 'models/marts/dim_customers.sql', /config\s*\(/i),
    },
    {
      id: 'run',
      prompt: 'Run `dbt run --select dim_customers` to verify it builds with the folder-level configuration.',
      hint: 'Run `dbt run --select dim_customers`.',
      validate: (s) => modelRan(s, 'dim_customers'),
    },
  ],
  quiz: {
    question: 'In dbt_project.yml, why do configuration properties start with a plus sign (+)?',
    options: [
      'To indicate they are required rather than optional',
      'To distinguish dbt configuration settings from folder path names in the directory tree',
      'To perform mathematical additions',
      'To signify that the setting applies only in production',
    ],
    correctIndex: 1,
    explanation: 'The `+` prefix tells dbt that the key is a model configuration (e.g. `+materialized: table`) rather than a nested folder name in the models directory hierarchy.',
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
    { label: 'Configuring models in dbt_project.yml', url: 'https://docs.getdbt.com/reference/model-configs#configuring-models' },
    { label: 'Model paths and hierarchy', url: 'https://docs.getdbt.com/reference/project-configs/model-paths' },
  ],
}

export default lesson28
