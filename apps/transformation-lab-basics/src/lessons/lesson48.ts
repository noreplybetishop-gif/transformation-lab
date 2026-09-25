import type { Lesson } from '../engine/types'
import { modelRan, fileContains } from '../engine/validators'
import { RAW_CUSTOMERS_CSV } from './_canonical'

const STG_CUSTOMERS = `select
    id as customer_id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}
`

const DIM_CUSTOMERS_V1 = `select
    customer_id,
    name,
    country
from {{ ref('stg_customers') }}
`

const DIM_CUSTOMERS_V2 = `select
    customer_id,
    name,
    lower(email) as email_cleaned,
    country,
    country = 'US' as is_domestic
from {{ ref('stg_customers') }}
`

const VERSIONS_YML = `version: 2

models:
  - name: dim_customers
    latest_version: 2
    versions:
      - v: 1
      - v: 2
`

const lesson48: Lesson = {
  id: 48,
  title: 'Model Versions & Zero-Downtime Migrations',
  concept: `When refactoring a central model, changing column names or business logic directly can instantly break dozens of downstream dashboards and operational systems.

**dbt Model Versions** allow analytics engineering teams to deprecate and introduce major schema versions gracefully:
\`\`\`yaml
models:
  - name: dim_customers
    latest_version: 2
    versions:
      - v: 1
      - v: 2
\`\`\`
The project stores both \`dim_customers_v1.sql\` and \`dim_customers_v2.sql\`.
- Downstream models calling \`ref('dim_customers')\` automatically resolve to the \`latest_version\` (v2).
- Legacy pipelines can pin to \`ref('dim_customers', v=1)\` until they are ready to upgrade!

This brings software engineering API versioning principles directly to data warehouse models.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: customers\n`,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
    'models/marts/dim_customers_v1.sql': DIM_CUSTOMERS_V1,
    'models/marts/dim_customers_v2.sql': DIM_CUSTOMERS_V2,
    'models/marts/_versions.yml': VERSIONS_YML,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  tasks: [
    {
      id: 'verify_versions',
      prompt: "Inspect `models/marts/_versions.yml` to verify that `latest_version: 2` is configured.",
      hint: "Check that latest_version: 2 and versions list v: 1 and v: 2 are present.",
      validate: (s) =>
        fileContains(s, 'models/marts/_versions.yml', 'latest_version: 2') &&
        fileContains(s, 'models/marts/_versions.yml', 'v: 1'),
    },
    {
      id: 'run_versions',
      prompt: 'Execute `dbt run --select dim_customers` to build both versions of the model.',
      hint: 'Run `dbt run --select dim_customers`.',
      validate: (s) => modelRan(s, 'dim_customers_v1') || modelRan(s, 'dim_customers_v2'),
    },
  ],
  quiz: {
    question: 'When a downstream model executes ref("dim_customers") without specifying a version number, which version is used?',
    options: [
      'Version 1 always',
      'The model defined by the latest_version property in YAML',
      'The model with the shortest query execution time',
      'It produces a compilation error',
    ],
    correctIndex: 1,
    explanation: 'dbt resolves unversioned ref() calls to whichever version is designated as latest_version in the model YAML configuration.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'stg_customers', label: 'stg_customers', layer: 'staging' },
        { id: 'dim_customers_v1', label: 'dim_customers (v1)', layer: 'mart' },
        { id: 'dim_customers_v2', label: 'dim_customers (v2)', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.customers', target: 'stg_customers' },
        { source: 'stg_customers', target: 'dim_customers_v1' },
        { source: 'stg_customers', target: 'dim_customers_v2' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Model Versions', url: 'https://docs.getdbt.com/docs/collaborate/govern/model-versions' },
  ],
}

export default lesson48
