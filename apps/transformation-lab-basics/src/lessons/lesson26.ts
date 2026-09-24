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
    amount_cents / 100.0 as amount
from {{ source('raw', 'payments') }}
`

const MACRO_GET_PAYMENT_METHODS = `{% macro get_payment_methods() %}
    {{ return(['credit_card', 'bank_transfer', 'gift_card']) }}
{% endmacro %}
`

const FCT_PIVOTED_PAYMENTS = `{% set payment_methods = ['credit_card', 'bank_transfer', 'gift_card'] %}

select
    order_id,
    {% for method in payment_methods %}
    sum(case when payment_method = '{{ method }}' then amount else 0 end) as {{ method }}_amount{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_payments') }}
group by order_id
`

const lesson26: Lesson = {
  id: 26,
  title: 'Macro Constants & Reusability',
  concept: `In the previous lab, you hardcoded the list of payment methods:

\`\`\`sql
{% set payment_methods = ['credit_card', 'bank_transfer', 'gift_card'] %}
\`\`\`

In a real enterprise project, several different models (finance marts, fraud detection models, order breakdowns) all need this exact list of payment methods.

If that list is copy-pasted across 6 models, what happens when your company launches **Apple Pay** or **PayPal**? An engineer will update 4 models and miss 2, causing silent discrepancies in executive dashboards.

The professional pattern is to centralize project-wide constants inside a macro using the **\`return()\`** function:

\`\`\`sql
-- macros/get_payment_methods.sql
{% macro get_payment_methods() %}
    {{ return(['credit_card', 'bank_transfer', 'gift_card']) }}
{% endmacro %}
\`\`\`

Now in any model, you simply call:

\`\`\`sql
{% set payment_methods = get_payment_methods() %}
\`\`\`

When a new payment method is added, you update a single file in \`macros/\`, and every dependent model in your DAG updates automatically!

In this lab, inspect \`macros/get_payment_methods.sql\`, update \`fct_pivoted_payments.sql\` to use it, and run dbt.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'macros/get_payment_methods.sql': MACRO_GET_PAYMENT_METHODS,
    'models/staging/stg_payments.sql': STG_PAYMENTS,
    'models/marts/fct_pivoted_payments.sql': FCT_PIVOTED_PAYMENTS,
  },
  seeds: {
    'raw.payments': RAW_PAYMENTS_CSV,
  },
  preRanModels: ['stg_payments'],
  tasks: [
    {
      id: 'macro_return',
      prompt: "Verify `macros/get_payment_methods.sql` uses `{{ return([...]) }}` to return the payment methods list.",
      hint: "Make sure `get_payment_methods` has `{{ return(['credit_card', 'bank_transfer', 'gift_card']) }}`.",
      validate: (s) =>
        fileMatches(s, 'macros/get_payment_methods.sql', /return\s*\(\s*\[.*credit_card.*\]\s*\)/i),
    },
    {
      id: 'call_macro',
      prompt: "In `models/marts/fct_pivoted_payments.sql`, replace the hardcoded list with `{% set payment_methods = get_payment_methods() %}`.",
      hint: "Change the first line to `{% set payment_methods = get_payment_methods() %}`.",
      validate: (s) =>
        modelSqlMatches(s, 'fct_pivoted_payments', /set\s+payment_methods\s*=\s*get_payment_methods\s*\(\s*\)/i),
    },
    {
      id: 'run',
      prompt: 'Run `dbt run --select fct_pivoted_payments` to verify the macro resolves properly.',
      hint: 'Type `dbt run --select fct_pivoted_payments`.',
      validate: (s) => modelRan(s, 'fct_pivoted_payments'),
    },
  ],
  quiz: {
    question: 'How do you return a Python list or dictionary from a Jinja macro in dbt?',
    options: [
      'Using the return keyword like Python: return [...]',
      'Using the Jinja {{ return(...) }} function call',
      'By printing JSON text to stdout',
      'Through a YAML configuration file',
    ],
    correctIndex: 1,
    explanation: 'dbt provides the `return(...)` Jinja function inside macros to return native Python objects (lists, strings, dicts) back to the caller.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.payments', label: 'raw.payments', layer: 'source' },
        { id: 'stg_payments', label: 'stg_payments', layer: 'staging' },
        { id: 'fct_pivoted_payments', label: 'fct_pivoted_payments', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.payments', target: 'stg_payments' },
        { source: 'stg_payments', target: 'fct_pivoted_payments' },
      ],
    },
  },
  furtherReading: [
    { label: 'Macro return values', url: 'https://docs.getdbt.com/docs/build/jinja-macros#returning-values' },
  ],
}

export default lesson26
