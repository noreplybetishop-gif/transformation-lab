import type { Lesson } from '../engine/types'
import { fileMatches, modelSqlMatches, modelRan } from '../engine/validators'
import { RAW_PAYMENTS_CSV } from './_canonical'

const SOURCES_YML = `version: 2

sources:
  - name: raw
    tables:
      - name: payments
`

const STG_PAYMENTS = `select
    id as payment_id,
    order_id,
    payment_method,
    amount_cents
from {{ source('raw', 'payments') }}
`

const MACRO_FORMAT_CURRENCY = `{% macro format_currency(column_name, divisor=100.0, decimal_places=2) %}
    round(({{ column_name }} / {{ divisor }}), {{ decimal_places }})
{% endmacro %}
`

const INT_PAYMENT_METRICS = `select
    payment_id,
    order_id,
    payment_method,
    amount_cents
from {{ ref('stg_payments') }}
`

const lesson24: Lesson = {
  id: 24,
  title: 'Parameterized Macros & Defaults',
  concept: `Macros become flexible tools when they accept parameters with sensible **default arguments**.

Just like in Python, Jinja allows you to specify defaults in the signature:

\`\`\`sql
{% macro format_currency(column_name, divisor=100.0, decimal_places=2) %}
    round(({{ column_name }} / {{ divisor }}), {{ decimal_places }})
{% endmacro %}
\`\`\`

Callers can now use this macro in several ways:

1. **Relying on defaults**:
   \`{{ format_currency('amount_cents') }}\`
   Divides by 100.0 and rounds to 2 decimal places.

2. **Custom precision**:
   \`{{ format_currency('amount_cents', decimal_places=4) }}\`
   Rounds to 4 decimal places for accounting reconciliation.

3. **Custom divisor**:
   \`{{ format_currency('amount_yen', divisor=1.0, decimal_places=0) }}\`

In this lab, inspect the \`format_currency\` macro and call it with \`decimal_places=4\` in \`int_payment_metrics.sql\`.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'macros/format_currency.sql': MACRO_FORMAT_CURRENCY,
    'models/staging/stg_payments.sql': STG_PAYMENTS,
    'models/intermediate/int_payment_metrics.sql': INT_PAYMENT_METRICS,
  },
  seeds: {
    'raw.payments': RAW_PAYMENTS_CSV,
  },
  preRanModels: ['stg_payments'],
  tasks: [
    {
      id: 'macro_signature',
      prompt: "Verify `macros/format_currency.sql` accepts `column_name, divisor=100.0, decimal_places=2`.",
      hint: "Check that `format_currency` has default values `divisor=100.0` and `decimal_places=2`.",
      validate: (s) =>
        fileMatches(s, 'macros/format_currency.sql', /format_currency\s*\(\s*column_name\s*,\s*divisor\s*=\s*100\.0\s*,\s*decimal_places\s*=\s*2\s*\)/i),
    },
    {
      id: 'use_param',
      prompt: "In `models/intermediate/int_payment_metrics.sql`, replace `amount_cents` with `{{ format_currency('amount_cents', decimal_places=4) }} as precise_amount`.",
      hint: "Call `{{ format_currency('amount_cents', decimal_places=4) }} as precise_amount` in the SELECT clause.",
      validate: (s) =>
        modelSqlMatches(s, 'int_payment_metrics', /format_currency\s*\(\s*['"]amount_cents['"]\s*,\s*decimal_places\s*=\s*4\s*\)\s+as\s+precise_amount/i),
    },
    {
      id: 'run',
      prompt: 'Run `dbt run --select int_payment_metrics` to verify execution.',
      hint: 'Type `dbt run --select int_payment_metrics` in the terminal.',
      validate: (s) => modelRan(s, 'int_payment_metrics'),
    },
  ],
  quiz: {
    question: 'What happens when a caller omits an argument that has a defined default in a Jinja macro?',
    options: [
      'dbt throws a compilation syntax error',
      'The argument adopts the specified default value without error',
      'The argument becomes null in the generated SQL',
      'dbt skips compiling the model',
    ],
    correctIndex: 1,
    explanation: 'Default arguments in Jinja macros evaluate to their fallback values automatically when not supplied by the caller.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.payments', label: 'raw.payments', layer: 'source' },
        { id: 'stg_payments', label: 'stg_payments', layer: 'staging' },
        { id: 'int_payment_metrics', label: 'int_payment_metrics', layer: 'intermediate' },
      ],
      edges: [
        { source: 'raw.payments', target: 'stg_payments' },
        { source: 'stg_payments', target: 'int_payment_metrics' },
      ],
    },
  },
  furtherReading: [
    { label: 'Jinja macro arguments and defaults', url: 'https://docs.getdbt.com/docs/build/jinja-macros#arguments' },
  ],
}

export default lesson24
