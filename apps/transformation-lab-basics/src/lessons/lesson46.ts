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

const DIM_CUSTOMERS_CONTRACT = `{{ config(
    materialized='table'
) }}

select
    customer_id,
    name,
    email,
    country
from {{ ref('stg_customers') }}
`

const CONTRACT_YML = `version: 2

models:
  - name: dim_customers_contract
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: integer
      - name: name
        data_type: varchar
      - name: email
        data_type: varchar
      - name: country
        data_type: varchar
`

const lesson46: Lesson = {
  id: 46,
  title: 'Model Contracts & Schema Enforcement',
  concept: `As data platforms scale to hundreds of contributors, downstream data consumers (financial reporting, machine learning, operational dashboards) can easily break when an upstream model accidentally drops or renames a column.

**dbt Model Contracts** guarantee strict schema stability:
\`\`\`yaml
models:
  - name: dim_customers_contract
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: integer
      - name: name
        data_type: varchar
      - name: email
        data_type: varchar
      - name: country
        data_type: varchar
\`\`\`
When \`contract: {enforced: true}\` is set, dbt performs strict pre-compilation checks:
1. Every column declared in the contract must exist in the model's \`SELECT\` list.
2. No extra, undeclared columns may be output by the query.
3. Every column's data type must match the contract.

If a developer breaks the contract, the model fails *before* writing invalid schemas into the warehouse!`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: customers\n`,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
    'models/marts/dim_customers_contract.sql': DIM_CUSTOMERS_CONTRACT,
    'models/marts/_contracts.yml': CONTRACT_YML,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  tasks: [
    {
      id: 'verify_contract',
      prompt: "Inspect `models/marts/_contracts.yml` to verify that `contract.enforced: true` and column `data_type`s are defined.",
      hint: "Check that contract: enforced: true is set under config.",
      validate: (s) =>
        fileContains(s, 'models/marts/_contracts.yml', 'contract') &&
        fileContains(s, 'models/marts/_contracts.yml', 'enforced: true'),
    },
    {
      id: 'build_contract_model',
      prompt: 'Execute `dbt build --select dim_customers_contract` to validate the contract and build the table.',
      hint: 'Run `dbt build --select dim_customers_contract`.',
      validate: (s) => modelRan(s, 'dim_customers_contract'),
    },
  ],
  quiz: {
    question: 'What happens during a dbt run if a model SQL query does not match an enforced contract?',
    options: [
      'dbt automatically guesses and alters the table columns',
      'dbt throws a compilation/build error and halts execution before committing changes',
      'dbt deletes the model file from git',
      'dbt ignores the contract silently',
    ],
    correctIndex: 1,
    explanation: 'Enforced contracts are strict guarantees: if the query output columns or data types deviate in any way from the contract specification, dbt fails the build immediately.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'stg_customers', label: 'stg_customers', layer: 'staging' },
        { id: 'dim_customers_contract', label: 'dim_customers_contract', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.customers', target: 'stg_customers' },
        { source: 'stg_customers', target: 'dim_customers_contract' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Model Contracts', url: 'https://docs.getdbt.com/docs/collaborate/govern/model-contracts' },
  ],
}

export default lesson46
