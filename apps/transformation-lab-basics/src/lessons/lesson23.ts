import type { Lesson } from '../engine/types'
import { fileMatches, modelSqlMatches, modelRan } from '../engine/validators'
import { RAW_PAYMENTS_CSV } from './_canonical'

const SOURCES_YML = `version: 2

sources:
  - name: raw
    tables:
      - name: payments
`

const MACRO_STARTER = `{% macro cents_to_dollars(column_name) %}
    round(({{ column_name }} / 100.0), 2)
{% endmacro %}
`

const STG_PAYMENTS = `select
    id as payment_id,
    order_id,
    payment_method,
    amount_cents
from {{ source('raw', 'payments') }}
`

const lesson23: Lesson = {
  id: 23,
  title: 'Your First Jinja Macro',
  concept: `In software development, DRY (Don't Repeat Yourself) is a golden rule. In plain SQL, however, analysts repeatedly copy-paste identical logic across dozens of queries:
- Converting cents to dollars (\`amount / 100.0\`).
- Shifting UTC timestamps to local business timezones.
- Truncating dates or stripping illegal characters.

If that logic ever changes, hunting down and updating 50 queries is tedious and error-prone.

In dbt, you encapsulate reusable SQL logic using **Macros**. Macros are functions written in Jinja that compile down into pure SQL.

They live in the \`macros/\` directory:

\`\`\`sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name) %}
    round(({{ column_name }} / 100.0), 2)
{% endmacro %}
\`\`\`

You can call your macro from any model like any function:

\`\`\`sql
select
    payment_id,
    {{ cents_to_dollars('amount_cents') }} as amount_dollars
from {{ ref('stg_payments') }}
\`\`\`

When dbt compiles the file, it replaces the Jinja call with clean, executable SQL.

In this lab, verify the \`cents_to_dollars\` macro and call it inside \`stg_payments.sql\`.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'macros/cents_to_dollars.sql': MACRO_STARTER,
    'models/staging/stg_payments.sql': STG_PAYMENTS,
  },
  seeds: {
    'raw.payments': RAW_PAYMENTS_CSV,
  },
  tasks: [
    {
      id: 'macro_defined',
      prompt: "Verify `macros/cents_to_dollars.sql` defines the `cents_to_dollars(column_name)` macro.",
      hint: "Make sure `macros/cents_to_dollars.sql` contains `{% macro cents_to_dollars(column_name) %}` and `{% endmacro %}`.",
      validate: (s) =>
        fileMatches(s, 'macros/cents_to_dollars.sql', /macro\s+cents_to_dollars\s*\(\s*column_name\s*\)/i),
    },
    {
      id: 'use_macro',
      prompt: "Use `{{ cents_to_dollars('amount_cents') }} as amount` in `models/staging/stg_payments.sql`.",
      hint: "Replace `amount_cents` with `{{ cents_to_dollars('amount_cents') }} as amount` in `stg_payments.sql`.",
      validate: (s) =>
        modelSqlMatches(s, 'stg_payments', /cents_to_dollars\s*\(\s*['"]amount_cents['"]\s*\)\s+as\s+amount/i),
    },
    {
      id: 'run',
      prompt: 'Run `dbt run --select stg_payments` to verify the macro compiles and materializes cleanly.',
      hint: 'Run `dbt run --select stg_payments`.',
      validate: (s) => modelRan(s, 'stg_payments'),
    },
  ],
  quiz: {
    question: 'Where should custom dbt macros be stored by convention?',
    options: [
      'In the models/ directory',
      'In the macros/ directory as .sql files',
      'In dbt_project.yml under macros:',
      'Inside tests/singular/',
    ],
    correctIndex: 1,
    explanation: 'dbt searches for macro definitions in files matching `macro-paths` (default `macros/`) ending with `.sql`.',
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
    { label: 'Writing Jinja macros', url: 'https://docs.getdbt.com/docs/build/jinja-macros' },
    { label: 'Macro properties and documentation', url: 'https://docs.getdbt.com/docs/build/macro-properties' },
  ],
}

export default lesson23
