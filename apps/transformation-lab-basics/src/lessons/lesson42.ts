import type { Lesson } from '../engine/types'
import { commandRan, fileContains } from '../engine/validators'
import { RAW_CUSTOMERS_CSV_DIRTY } from './_canonical'

const STG_CUSTOMERS = `select
    id as customer_id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}
`

const lesson42: Lesson = {
  id: 42,
  title: 'Storing Test Failures for Auditing',
  concept: `When a dbt test fails, the terminal traditionally prints:
\`Got 1 result, configured to fail if != 0\`.

To find *which* specific rows failed, engineers historically had to copy the compiled SQL out of \`target/compiled/...\` and run it manually in a SQL editor.

With **\`store_failures\`**, dbt does this automatically:
\`\`\`sql
{{ config(store_failures = true) }}
\`\`\`
Or globally in \`dbt_project.yml\`:
\`\`\`yaml
tests:
  +store_failures: true
  +schema: test_audit
\`\`\`
When a test fails, dbt materializes the exact failing rows into an audit table in your warehouse! Data engineers and automated alert bots can directly query \`dbt_test__audit.<test_name>\` to triage bad data instantaneously.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: customers\n`,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
    'tests/assert_valid_customer_emails.sql': `-- Flag customers with null or empty emails\nselect\n    customer_id,\n    name\nfrom {{ ref('stg_customers') }}\nwhere email is null or email = ''\n`,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV_DIRTY,
  },
  tasks: [
    {
      id: 'configure_store_failures',
      prompt: "Add `{{ config(store_failures=true) }}` to `tests/assert_valid_customer_emails.sql`.",
      hint: "Add {{ config(store_failures=true) }} at the top of the test file.",
      validate: (s) =>
        fileContains(s, 'tests/assert_valid_customer_emails.sql', 'store_failures') &&
        fileContains(s, 'tests/assert_valid_customer_emails.sql', 'true'),
    },
    {
      id: 'run_test_audit',
      prompt: 'Execute `dbt test --select assert_valid_customer_emails` to store the failure audit records.',
      hint: 'Run `dbt test --select assert_valid_customer_emails`.',
      validate: (s) => commandRan(s, 'test') || commandRan(s, 'build'),
    },
  ],
  quiz: {
    question: 'Where does dbt write the offending rows when store_failures is enabled on a data test?',
    options: [
      'Into a temporary file on the local developer machine only',
      'Directly into an audit schema inside the target warehouse database',
      'It sends them directly to an email distribution list',
      'It appends them to the git commit message',
    ],
    correctIndex: 1,
    explanation: 'dbt creates a persistent audit table in the warehouse (by default in your target schema + _dbt_test__audit) holding the exact rows returned by the test query.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'stg_customers', label: 'stg_customers', layer: 'staging' },
      ],
      edges: [
        { source: 'raw.customers', target: 'stg_customers' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt store_failures', url: 'https://docs.getdbt.com/reference/resource-configs/store_failures' },
  ],
}

export default lesson42
