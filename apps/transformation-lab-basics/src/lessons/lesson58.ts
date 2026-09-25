import type { Lesson } from '../engine/types'
import { commandRan, fileContains } from '../engine/validators'
import { RAW_ORDERS_CSV } from './_canonical'

const PROFILES_YML = `transformation_lab:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /project/warehouse.duckdb
      threads: 1
    ci:
      type: duckdb
      path: /project/warehouse.duckdb
      threads: 1
    prod:
      type: duckdb
      path: /project/warehouse.duckdb
      threads: 4
`

const STG_ORDERS = `select
    id as order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ source('raw', 'orders') }}
`

const lesson58: Lesson = {
  id: 58,
  title: 'Environment Separation: Profiles & Targets',
  concept: `Analytics engineering code must run seamlessly across multiple runtime environments: local developer workstations, pull request CI runners, and scheduled production orchestrators.

In dbt, this environment separation is managed in **\`profiles.yml\`** using **targets**:
\`\`\`yaml
transformation_lab:
  target: dev
  outputs:
    dev:
      type: snowflake
      schema: dbt_alice
      threads: 4
    ci:
      type: snowflake
      schema: ci_pr_123
      threads: 8
    prod:
      type: snowflake
      schema: analytics
      threads: 16
      password: "{{ env_var('DBT_PROD_PASSWORD') }}"
\`\`\`
Key rules:
1. **Never commit secrets**: Use Jinja's \`{{ env_var('...') }}\` to dynamically inject warehouse passwords and service account keys.
2. **Switch targets on the fly**: Running with \`--target prod\` or \`--target dev\` lets models dynamically adapt their logic, schema names, and resource allocations.`,
  initialFiles: {
    'profiles.yml': PROFILES_YML,
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: orders\n`,
    'models/staging/stg_orders.sql': STG_ORDERS,
  },
  seeds: {
    'raw.orders': RAW_ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_profiles',
      prompt: "Inspect `profiles.yml` to verify that `dev:`, `ci:`, and `prod:` targets are configured.",
      hint: "Check that outputs contains dev, ci, and prod.",
      validate: (s) =>
        fileContains(s, 'profiles.yml', 'dev:') &&
        fileContains(s, 'profiles.yml', 'prod:'),
    },
    {
      id: 'compile_with_target',
      prompt: 'Execute `dbt compile --target dev` to compile the project targeting the dev environment.',
      hint: 'Type `dbt compile --target dev` in the console.',
      validate: (s) => commandRan(s, 'compile') || commandRan(s, 'build'),
    },
  ],
  quiz: {
    question: 'How should production database passwords and private keys be referenced in profiles.yml?',
    options: [
      'Hardcoded directly in plain text in git commits',
      'Loaded dynamically using the env_var() Jinja function from environment variables',
      'Written as SQL comments in models',
      'Stored in the public README.md file',
    ],
    correctIndex: 1,
    explanation: 'Using {{ env_var("...") }} keeps sensitive database credentials out of version control, sourcing them safely from secret managers or CI environment variables at runtime.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.orders', label: 'raw.orders', layer: 'source' },
        { id: 'stg_orders', label: 'stg_orders', layer: 'staging' },
      ],
      edges: [
        { source: 'raw.orders', target: 'stg_orders' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Connection Profiles', url: 'https://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles' },
  ],
}

export default lesson58
