import type { Lesson } from '../engine/types'
import { fileMatches, testPassed } from '../engine/validators'
import { RAW_PAYMENTS_CSV } from './_canonical'

const SOURCES_YML = `version: 2

sources:
  - name: raw
    tables:
      - name: payments
`

const MACRO_TEST_IS_POSITIVE = `{% test is_positive(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}
`

const STG_PAYMENTS = `select
    id as payment_id,
    order_id,
    payment_method,
    amount_cents / 100.0 as amount
from {{ source('raw', 'payments') }}
`

const STAGING_SCHEMA_YML = `version: 2

models:
  - name: stg_payments
    description: "Payment records from the checkout gateway."
    columns:
      - name: payment_id
        data_tests:
          - not_null
          - unique
      - name: amount
        description: "Payment amount in dollars; must be strictly positive."
        data_tests:
          - not_null
`

const lesson27: Lesson = {
  id: 27,
  title: 'Custom Generic Test Macros',
  concept: `In the Basics course, you used dbt's 4 built-in generic tests: \`unique\`, \`not_null\`, \`accepted_values\`, and \`relationships\`.

What if you need a business rule that isn't built into dbt? For example:
- Verifying that order amounts are strictly positive (\`amount > 0\`).
- Checking that percentages are between 0 and 100.
- Ensuring end dates are after start dates.

A **singular test** in \`tests/my_test.sql\` works, but it's hardcoded to one specific table and column.

To create a test you can reuse across *any* column on *any* model in your project, write a **Custom Generic Test**!

A custom generic test is simply a macro defined with \`{% test ... %}\`:

\`\`\`sql
-- macros/test_is_positive.sql
{% test is_positive(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}
\`\`\`

Remember the dbt test contract:
- The query returns rows that **violate** the rule (bad rows).
- If the query returns **0 rows**, the test **PASSES**.
- If it returns **any rows**, the test **FAILS**.

Once defined, you apply it in your schema YAML just like \`not_null\`:

\`\`\`yaml
columns:
  - name: amount
    data_tests:
      - not_null
      - is_positive
\`\`\`

In this lab, inspect \`macros/test_is_positive.sql\`, attach \`is_positive\` to \`amount\` in \`_schema.yml\`, and run \`dbt test\`.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'macros/test_is_positive.sql': MACRO_TEST_IS_POSITIVE,
    'models/staging/stg_payments.sql': STG_PAYMENTS,
    'models/staging/_schema.yml': STAGING_SCHEMA_YML,
  },
  seeds: {
    'raw.payments': RAW_PAYMENTS_CSV,
  },
  preRanModels: ['stg_payments'],
  tasks: [
    {
      id: 'verify_test_macro',
      prompt: "Verify `macros/test_is_positive.sql` defines the generic test `{% test is_positive(model, column_name) %}`.",
      hint: "Make sure `macros/test_is_positive.sql` contains `{% test is_positive(model, column_name) %}` and selects rows `<= 0`.",
      validate: (s) =>
        fileMatches(s, 'macros/test_is_positive.sql', /test\s+is_positive\s*\(\s*model\s*,\s*column_name\s*\)/i),
    },
    {
      id: 'attach_test',
      prompt: "Add `- is_positive` to `data_tests:` under `amount` in `models/staging/_schema.yml`.",
      hint: "Under `columns: - name: amount data_tests:`, add `- is_positive` below `- not_null`.",
      validate: (s) =>
        fileMatches(s, 'models/staging/_schema.yml', /is_positive/i),
    },
    {
      id: 'run_test',
      prompt: 'Run `dbt test --select stg_payments` to execute your custom test.',
      hint: 'Type `dbt test --select stg_payments` in the terminal.',
      validate: (s) => testPassed(s, 'stg_payments'),
    },
  ],
  quiz: {
    question: 'How does dbt determine if a custom generic test has passed or failed?',
    options: [
      'The query must return a single boolean TRUE value',
      'The query must return 0 rows for pass, and >0 rows for fail',
      'The query must raise an explicit SQL exception on failure',
      'The query must return the string "PASS"',
    ],
    correctIndex: 1,
    explanation: 'All dbt tests (generic and singular) query for violating records. If 0 records are returned, the test passes. If 1 or more rows are returned, it fails.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.payments', label: 'raw.payments', layer: 'source' },
        { id: 'stg_payments', label: 'stg_payments', layer: 'staging' },
      ],
      edges: [
        { source: 'raw.payments', target: 'stg_payments' },
      ],
    },
  },
  furtherReading: [
    { label: 'Writing custom generic tests', url: 'https://docs.getdbt.com/docs/build/custom-tests#generic-test-definition' },
    { label: 'dbt test command', url: 'https://docs.getdbt.com/reference/commands/test' },
  ],
}

export default lesson27
