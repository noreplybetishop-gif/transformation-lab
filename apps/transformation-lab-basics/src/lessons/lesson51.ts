import type { Lesson } from '../engine/types'
import { modelCompiled, fileContains } from '../engine/validators'
import { RAW_PAYMENTS_CSV } from './_canonical'

const MACRO_GENERATE_SCHEMA = `{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- elif target.name == 'prod' -%}

        {{ custom_schema_name | trim }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
`

const FCT_FINANCE_REVENUE = `{{ config(
    schema='finance'
) }}

select
    payment_method,
    count(*) as payment_count,
    sum(amount_cents) / 100.0 as total_amount
from {{ source('raw', 'payments') }}
group by payment_method
`

const lesson51: Lesson = {
  id: 51,
  title: 'Custom Schema Routing Macro',
  concept: `By default, when you configure \`schema: finance\` in a model, dbt concatenates your default schema: \`<default_schema>_finance\` (e.g. \`main_finance\`).

In production, enterprise teams want models to write directly to clean, dedicated schemas like \`finance\`, \`marketing\`, or \`analytics\`, while in development they still want isolated schemas like \`dbt_alice_finance\`.

You control this completely by overriding dbt's built-in **\`generate_schema_name\`** macro in \`macros/generate_schema_name.sql\`:
\`\`\`sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
\`\`\`
In this lab, you will inspect this enterprise macro and compile a model utilizing custom schema routing.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: payments\n`,
    'macros/generate_schema_name.sql': MACRO_GENERATE_SCHEMA,
    'models/marts/fct_finance_revenue.sql': FCT_FINANCE_REVENUE,
  },
  seeds: {
    'raw.payments': RAW_PAYMENTS_CSV,
  },
  tasks: [
    {
      id: 'verify_macro',
      prompt: "Inspect `macros/generate_schema_name.sql` to verify custom schema resolution logic.",
      hint: "Check that target.name == 'prod' returns custom_schema_name directly.",
      validate: (s) =>
        fileContains(s, 'macros/generate_schema_name.sql', 'generate_schema_name') &&
        fileContains(s, 'macros/generate_schema_name.sql', 'default_schema'),
    },
    {
      id: 'compile_model',
      prompt: 'Execute `dbt compile --select fct_finance_revenue` to test schema generation.',
      hint: 'Run `dbt compile --select fct_finance_revenue`.',
      validate: (s) => modelCompiled(s, 'fct_finance_revenue'),
    },
  ],
  quiz: {
    question: 'Why does overriding generate_schema_name matter for enterprise production deployments?',
    options: [
      'It creates database user logins',
      'It allows writing directly into clean domain schemas in production while isolating personal developer schemas in dev',
      'It accelerates download speeds for Pyodide',
      'It prevents dbt from creating any SQL queries',
    ],
    correctIndex: 1,
    explanation: 'By checking target.name, teams can route production models to clean business domain schemas (e.g. finance) while namespacing development builds to prevent developers from overwriting each other.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.payments', label: 'raw.payments', layer: 'source' },
        { id: 'fct_finance_revenue', label: 'fct_finance_revenue (finance schema)', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.payments', target: 'fct_finance_revenue' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Custom Schemas', url: 'https://docs.getdbt.com/docs/build/custom-schemas' },
  ],
}

export default lesson51
