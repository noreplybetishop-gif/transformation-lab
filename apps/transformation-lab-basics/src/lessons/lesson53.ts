import type { Lesson } from '../engine/types'
import { modelRan, fileContains } from '../engine/validators'
import { RAW_CUSTOMERS_CSV } from './_canonical'

const DIM_AUDITED_CUSTOMERS = `{{ config(
    materialized='table',
    post_hook=[
      "-- Post-hook audit statement: record build completion or grant permissions",
      "select 1"
    ]
) }}

select
    id as customer_id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}
`

const lesson53: Lesson = {
  id: 53,
  title: 'Pre-Hooks & Post-Hooks',
  concept: `Real data warehouse environments require operations beyond \`CREATE TABLE AS SELECT\`:
- Granting \`SELECT\` permissions to reporting roles (\`GRANT SELECT ON ... TO ROLE bi_users\`)
- Vacuuming or optimizing table storage
- Inserting metadata entries into orchestration audit tables

dbt enables this through **Pre-Hooks** and **Post-Hooks**:
\`\`\`sql
{{ config(
    materialized='table',
    pre_hook="-- Runs immediately before the model builds",
    post_hook=[
      "grant select on {{ this }} to role reporting",
      "insert into audit.model_runs values ('{{ this.name }}', current_timestamp)"
    ]
) }}
\`\`\`
Hooks can be configured directly inside a single model's \`config()\`, or defined project-wide in \`dbt_project.yml\` to apply across entire folders automatically.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: customers\n`,
    'models/marts/dim_audited_customers.sql': DIM_AUDITED_CUSTOMERS,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  tasks: [
    {
      id: 'verify_post_hook',
      prompt: "Inspect `models/marts/dim_audited_customers.sql` and verify that `post_hook` is configured inside `config()`.",
      hint: "Check that post_hook=[...] is present in the model config.",
      validate: (s) =>
        fileContains(s, 'models/marts/dim_audited_customers.sql', 'post_hook'),
    },
    {
      id: 'run_model_with_hook',
      prompt: 'Execute `dbt run --select dim_audited_customers` to execute the model and its post-hook.',
      hint: 'Run `dbt run --select dim_audited_customers`.',
      validate: (s) => modelRan(s, 'dim_audited_customers'),
    },
  ],
  quiz: {
    question: 'When does a post_hook configured in a model execute?',
    options: [
      'Before the dbt project begins compiling',
      'Immediately after the model finishes building in the warehouse',
      'Only when a data test fails',
      'Only during dbt seed',
    ],
    correctIndex: 1,
    explanation: 'A model post_hook runs in the same transaction or immediately following the model creation query, ideal for permissions, vacuuming, and audit logging.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'dim_audited_customers', label: 'dim_audited_customers (post-hook)', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.customers', target: 'dim_audited_customers' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Pre-hooks and Post-hooks', url: 'https://docs.getdbt.com/reference/resource-configs/pre-hook-post-hook' },
  ],
}

export default lesson53
