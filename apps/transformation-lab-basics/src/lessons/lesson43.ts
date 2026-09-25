import type { Lesson } from '../engine/types'
import { commandRan, fileContains } from '../engine/validators'
import { RAW_PAYMENTS_CSV } from './_canonical'

const STG_PAYMENTS = `select
    id as payment_id,
    order_id,
    payment_method,
    amount_cents / 100.0 as amount
from {{ source('raw', 'payments') }}
`

const MACRO_IS_BETWEEN = `{% test is_between(model, column_name, min_val, max_val) %}

select
    {{ column_name }} as failing_value
from {{ model }}
where {{ column_name }} < {{ min_val }}
   or {{ column_name }} > {{ max_val }}

{% endtest %}
`

const lesson43: Lesson = {
  id: 43,
  title: 'Custom Generic Test Macros with Arguments',
  concept: `The four out-of-the-box generic tests (\`unique\`, \`not_null\`, \`accepted_values\`, and \`relationships\`) cover basic sanity. However, enterprise teams require domain-specific generic tests: range assertions, regex validations, date freshness, and statistical bounds.

A custom generic test is declared in a \`macros/\` file using the \`{% test ... %}\` block:
\`\`\`sql
{% test is_between(model, column_name, min_val, max_val) %}

select
    {{ column_name }} as invalid_value
from {{ model }}
where {{ column_name }} < {{ min_val }}
   or {{ column_name }} > {{ max_val }}

{% endtest %}
\`\`\`
Any test that returns **0 rows passes**; any rows returned indicate violations. Once saved in \`macros/\`, any model column in \`_schema.yml\` can invoke it just like dbt's built-in tests!`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: payments\n`,
    'models/staging/stg_payments.sql': STG_PAYMENTS,
    'macros/test_is_between.sql': MACRO_IS_BETWEEN,
    'models/staging/_schema.yml': `version: 2\n\nmodels:\n  - name: stg_payments\n    columns:\n      - name: amount\n        data_tests:\n          - not_null\n          # Add is_between test here\n`,
  },
  seeds: {
    'raw.payments': RAW_PAYMENTS_CSV,
  },
  tasks: [
    {
      id: 'apply_generic_test',
      prompt: "Add the `is_between` test to column `amount` in `models/staging/_schema.yml` with `min_val: 0` and `max_val: 500`.",
      hint: "Under data_tests, add:\n  - is_between:\n      arguments:\n        min_val: 0\n        max_val: 500",
      validate: (s) =>
        fileContains(s, 'models/staging/_schema.yml', 'is_between') &&
        fileContains(s, 'models/staging/_schema.yml', 'min_val') &&
        fileContains(s, 'models/staging/_schema.yml', 'max_val'),
    },
    {
      id: 'run_test',
      prompt: 'Execute `dbt test --select stg_payments` to run your new custom generic test.',
      hint: 'Run `dbt test --select stg_payments`.',
      validate: (s) => commandRan(s, 'test') || commandRan(s, 'build'),
    },
  ],
  quiz: {
    question: 'What convention does dbt use to distinguish a generic data test from a standard Jinja macro?',
    options: [
      'It must begin with {% test name(model, column_name, ...) %} and end with {% endtest %}',
      'It must be written in Python instead of SQL',
      'The file name must begin with test_and_run_',
      'It must be stored in the seeds/ folder',
    ],
    correctIndex: 0,
    explanation: 'Wrapping the macro in {% test name(...) %} {% endtest %} registers it in the dbt compilation graph as a reusable generic test callable in YAML schema files.',
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
    { label: 'dbt Custom Data Tests', url: 'https://docs.getdbt.com/docs/build/data-tests#custom-data-tests' },
  ],
}

export default lesson43
