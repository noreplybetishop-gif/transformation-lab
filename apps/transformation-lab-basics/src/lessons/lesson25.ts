import type { Lesson } from '../engine/types'
import { modelSqlMatches, modelCompiled, modelRan } from '../engine/validators'
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

const FCT_PIVOTED_PAYMENTS = `{% set payment_methods = ['credit_card', 'bank_transfer', 'gift_card'] %}

select
    order_id,
    {% for method in payment_methods %}
    sum(case when payment_method = '{{ method }}' then amount else 0 end) as {{ method }}_amount{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_payments') }}
group by order_id
`

const lesson25: Lesson = {
  id: 25,
  title: 'Jinja Loops & Dynamic SQL',
  concept: `One of dbt's most powerful capabilities is writing **dynamic SQL** using Jinja control structures like \`{% for %}\` loops.

A classic example is pivoting categorical rows into columns (e.g. calculating total spend per payment method per order).

In raw SQL, you are forced to write tedious, repetitive boilerplate:

\`\`\`sql
sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount,
sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
sum(case when payment_method = 'gift_card' then amount else 0 end) as gift_card_amount
\`\`\`

If there are 20 payment methods, you have to write 20 identical lines - and update every query when a new method is added.

With a Jinja loop, dbt generates the SQL for you:

\`\`\`sql
{% set payment_methods = ['credit_card', 'bank_transfer', 'gift_card'] %}

select
    order_id,
    {% for method in payment_methods %}
    sum(case when payment_method = '{{ method }}' then amount else 0 end) as {{ method }}_amount{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_payments') }}
group by order_id
\`\`\`

Notice the **\`loop.last\`** check: Jinja provides loop context variables. \`loop.last\` evaluates to \`false\` on every iteration except the final one, guaranteeing no trailing comma breaks your SQL syntax!

In this lab, inspect the loop in \`fct_pivoted_payments.sql\`, compile it to see the generated SQL, and run it.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/stg_payments.sql': STG_PAYMENTS,
    'models/marts/fct_pivoted_payments.sql': FCT_PIVOTED_PAYMENTS,
  },
  seeds: {
    'raw.payments': RAW_PAYMENTS_CSV,
  },
  preRanModels: ['stg_payments'],
  tasks: [
    {
      id: 'loop_check',
      prompt: "Ensure `models/marts/fct_pivoted_payments.sql` contains a `{% for method in payment_methods %}` loop with `loop.last` comma handling.",
      hint: "Check that `fct_pivoted_payments.sql` contains `{% for method in payment_methods %}` and `{% if not loop.last %},{% endif %}`.",
      validate: (s) =>
        modelSqlMatches(s, 'fct_pivoted_payments', /for\s+method\s+in\s+payment_methods/i) &&
        modelSqlMatches(s, 'fct_pivoted_payments', /loop\.last/i),
    },
    {
      id: 'compile',
      prompt: 'Run `dbt compile --select fct_pivoted_payments` to see how dbt unrolls the loop into pure SQL.',
      hint: 'Type `dbt compile --select fct_pivoted_payments`.',
      validate: (s) => modelCompiled(s, 'fct_pivoted_payments'),
    },
    {
      id: 'run',
      prompt: 'Run `dbt run --select fct_pivoted_payments` to materialize the pivoted mart in DuckDB.',
      hint: 'Run `dbt run --select fct_pivoted_payments`.',
      validate: (s) => modelRan(s, 'fct_pivoted_payments'),
    },
  ],
  quiz: {
    question: 'What is the purpose of the Jinja loop.last variable in SQL templating?',
    options: [
      'It breaks out of the loop early',
      'It allows suppressing the trailing comma on the final column expression to prevent SQL syntax errors',
      'It counts how many rows were inserted into the database',
      'It returns the last model in the DAG',
    ],
    correctIndex: 1,
    explanation: '`loop.last` is true only on the final element of a loop, allowing you to omit trailing commas in SELECT lists or UNION statements.',
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
    { label: 'Jinja for-loops in dbt', url: 'https://docs.getdbt.com/docs/build/jinja-macros#for-loops' },
    { label: 'Pivoting columns with dbt', url: 'https://docs.getdbt.com/blog/pivot-dbt-sql' },
  ],
}

export default lesson25
