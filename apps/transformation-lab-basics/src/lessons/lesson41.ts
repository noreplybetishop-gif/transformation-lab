import type { Lesson } from '../engine/types'
import { commandRan, fileContains } from '../engine/validators'
import { RAW_ORDERS_CSV } from './_canonical'

const STG_ORDERS = `select
    id as order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ source('raw', 'orders') }}
`

const lesson41: Lesson = {
  id: 41,
  title: 'Test Severity & Error Thresholds',
  concept: `In production pipelines, not every data quality finding should stop the pipeline or wake up the on-call engineer at 3 AM.

dbt allows configuring granular test thresholds via \`severity\`, \`warn_if\`, and \`error_if\`:
\`\`\`sql
{{ config(
    severity = 'warn',
    warn_if = '> 0',
    error_if = '> 50'
) }}
\`\`\`
- If failing records are between 1 and 50, dbt prints a \`WARN\` message in the log and exits with code 0 (success).
- Only when failures exceed 50 does dbt elevate the result to \`ERROR\` and stop downstream builds.

This allows data teams to track data quality drift, detect anomalies early, and establish tiered SLA responses.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: orders\n`,
    'models/staging/stg_orders.sql': STG_ORDERS,
    'tests/assert_high_value_orders.sql': `-- Add severity config with warn_if and error_if thresholds\n\nselect\n    order_id,\n    amount\nfrom {{ ref('stg_orders') }}\nwhere amount > 50.00\n`,
  },
  seeds: {
    'raw.orders': RAW_ORDERS_CSV,
  },
  tasks: [
    {
      id: 'configure_severity',
      prompt: "Configure `tests/assert_high_value_orders.sql` with `{{ config(severity='warn') }}`.",
      hint: "Add {{ config(severity='warn') }} at the top of the test file.",
      validate: (s) =>
        fileContains(s, 'tests/assert_high_value_orders.sql', 'severity') &&
        fileContains(s, 'tests/assert_high_value_orders.sql', 'warn'),
    },
    {
      id: 'run_test',
      prompt: 'Execute `dbt test --select assert_high_value_orders` to observe the warning in action.',
      hint: 'Run `dbt test --select assert_high_value_orders`.',
      validate: (s) => commandRan(s, 'test') || commandRan(s, 'build'),
    },
  ],
  quiz: {
    question: 'What is the primary benefit of configuring severity: warn on a dbt data test?',
    options: [
      'It stops all transformation models immediately',
      'It flags anomalies for data teams without unnecessarily halting production pipeline execution',
      'It automatically repairs the corrupted database records',
      'It turns off testing for all staging models',
    ],
    correctIndex: 1,
    explanation: 'Setting severity: warn alerts the engineering team to anomalous records while allowing the remaining downstream DAG to continue building without breaking mission-critical reporting.',
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
    { label: 'dbt Test configs', url: 'https://docs.getdbt.com/reference/resource-configs/severity' },
  ],
}

export default lesson41
