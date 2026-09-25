import type { Lesson } from '../engine/types'
import { modelCompiled, fileContains } from '../engine/validators'
import { RAW_CUSTOMERS_CSV } from './_canonical'

const STG_CUSTOMERS = `select
    id as customer_id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}
`

const DIM_CUSTOMERS_GOVERNED = `select
    customer_id,
    name,
    country
from {{ ref('stg_customers') }}
`

const GOVERNANCE_YML = `version: 2

groups:
  - name: core_analytics
    owner:
      email: data-core@example.com

models:
  - name: dim_customers_governed
    config:
      group: core_analytics
      access: public
    description: "Publicly accessible customer dimension contract for multi-domain analytics"
`

const lesson47: Lesson = {
  id: 47,
  title: 'Model Access & Multi-Project Governance',
  concept: `In enterprise organizations with multiple teams (e.g., Marketing, Finance, Product, Core Data), building in a single monolithic dbt project leads to merge conflicts, slow builds, and tight coupling.

dbt Mesh introduces multi-project governance via **Model Access Modifiers**:
- **\`access: private\`**: Model can only be referenced by other models within the same dbt group.
- **\`access: protected\`**: (Default) Model can be referenced anywhere within the same dbt project.
- **\`access: public\`**: Model acts as an official public API and can be referenced across different projects and domains via \`ref('project_name', 'model_name')\`!

Pairing \`access: public\` with group ownership and model contracts enables true decentralized Data Mesh architectures.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: customers\n`,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
    'models/marts/dim_customers_governed.sql': DIM_CUSTOMERS_GOVERNED,
    'models/marts/_governance.yml': GOVERNANCE_YML,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  tasks: [
    {
      id: 'verify_access_config',
      prompt: "Inspect `models/marts/_governance.yml` to verify that `access: public` and `group: core_analytics` are assigned to `dim_customers_governed`.",
      hint: "Check that access: public and group: core_analytics are configured under config.",
      validate: (s) =>
        fileContains(s, 'models/marts/_governance.yml', 'access: public') &&
        fileContains(s, 'models/marts/_governance.yml', 'core_analytics'),
    },
    {
      id: 'compile_governed_model',
      prompt: 'Execute `dbt compile --select dim_customers_governed` to validate the governance graph.',
      hint: 'Run `dbt compile --select dim_customers_governed`.',
      validate: (s) => modelCompiled(s, 'dim_customers_governed'),
    },
  ],
  quiz: {
    question: 'Which access modifier allows a dbt model to be referenced by downstream dbt projects in a Data Mesh?',
    options: [
      'access: internal',
      'access: public',
      'access: shared',
      'access: global',
    ],
    correctIndex: 1,
    explanation: 'access: public designates a model as an official cross-project interface, enabling cross-project references in dbt Mesh architectures.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'stg_customers', label: 'stg_customers', layer: 'staging' },
        { id: 'dim_customers_governed', label: 'dim_customers_governed (public)', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.customers', target: 'stg_customers' },
        { source: 'stg_customers', target: 'dim_customers_governed' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Model Access', url: 'https://docs.getdbt.com/docs/collaborate/govern/model-access' },
  ],
}

export default lesson47
