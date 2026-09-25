import type { Lesson } from '../engine/types'
import { modelCompiled, fileContains } from '../engine/validators'
import { RAW_PAYMENTS_CSV } from './_canonical'

const MACRO_GET_METHODS = `{% macro get_payment_methods() %}
    {% set query %}
        select distinct payment_method from {{ source('raw', 'payments') }} order by 1
    {% endset %}

    {% if execute %}
        {% set results = run_query(query) %}
        {% set methods = results.columns[0].values() %}
    {% else %}
        {% set methods = [] %}
    {% endif %}

    {{ return(methods) }}
{% endmacro %}
`

const FCT_PIVOT_PAYMENTS = `{% set methods = ['credit_card', 'bank_transfer', 'gift_card'] %}

select
    order_id,
    {% for method in methods %}
    sum(case when payment_method = '{{ method }}' then amount_cents else 0 end) / 100.0 as {{ method }}_amount
    {% if not loop.last %},{% endif %}
    {% endfor %}
from {{ source('raw', 'payments') }}
group by order_id
`

const lesson52: Lesson = {
  id: 52,
  title: 'Database Introspection with run_query',
  concept: `Hardcoding lists of categorical values (like payment methods or countries) in SQL means your models go out of date every time the business adds a new option.

With **\`run_query()\`**, Jinja can query the database directly during compilation:
\`\`\`sql
{% set query %}
    select distinct payment_method from {{ source('raw', 'payments') }} order by 1
{% endset %}

{% if execute %}
    {% set results = run_query(query) %}
    {% set payment_methods = results.columns[0].values() %}
{% else %}
    {% set payment_methods = [] %}
{% endif %}
\`\`\`
**Crucial Rule**: You must always wrap \`run_query\` in **\`{% if execute %}\`**! During dbt's initial parse phase, the warehouse is not yet connected; only during execution phase does \`run_query\` return an \`agate\` Table object whose columns and values you can loop over dynamically.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: payments\n`,
    'macros/get_payment_methods.sql': MACRO_GET_METHODS,
    'models/marts/fct_pivot_payments.sql': FCT_PIVOT_PAYMENTS,
  },
  seeds: {
    'raw.payments': RAW_PAYMENTS_CSV,
  },
  tasks: [
    {
      id: 'verify_introspection_macro',
      prompt: "Inspect `macros/get_payment_methods.sql` to verify the `run_query` and `if execute` block.",
      hint: "Check that run_query(query) and if execute are used correctly.",
      validate: (s) =>
        fileContains(s, 'macros/get_payment_methods.sql', 'run_query') &&
        fileContains(s, 'macros/get_payment_methods.sql', 'if execute'),
    },
    {
      id: 'compile_dynamic_pivot',
      prompt: 'Execute `dbt compile --select fct_pivot_payments` to see dynamic SQL generation.',
      hint: 'Run `dbt compile --select fct_pivot_payments`.',
      validate: (s) => modelCompiled(s, 'fct_pivot_payments'),
    },
  ],
  quiz: {
    question: 'Why must calls to run_query() always be protected by {% if execute %} in Jinja?',
    options: [
      'To prevent SQL injection attacks',
      'Because during the initial parse stage dbt constructs the DAG without querying the database, so run_query would error',
      'To force the database to use SSL encryption',
      'Because DuckDB does not support Jinja',
    ],
    correctIndex: 1,
    explanation: 'dbt has two phases: parse (building the manifest and DAG) and execute (running queries). The execute variable is only true during the second phase, preventing runtime exceptions during project parsing.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.payments', label: 'raw.payments', layer: 'source' },
        { id: 'fct_pivot_payments', label: 'fct_pivot_payments', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.payments', target: 'fct_pivot_payments' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt run_query macro', url: 'https://docs.getdbt.com/reference/dbt-jinja-functions/run_query' },
  ],
}

export default lesson52
