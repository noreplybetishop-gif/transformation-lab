import type { Lesson } from '../engine/types'
import { modelRan, fileContains } from '../engine/validators'
import { RAW_ORDERS_CSV } from './_canonical'

const STG_ORDERS = `select
    id as order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ source('raw', 'orders') }}
`

const FCT_REVENUE_DEPLOY = `{{ config(
    materialized='table'
) }}

select
    customer_id,
    sum(amount) as total_revenue
from {{ ref('stg_orders') }}
where status = 'paid'
group by customer_id
`

const MACRO_BLUE_GREEN = `{% macro blue_green_swap(source_schema, target_schema) %}
    {% set swap_sql %}
        -- In warehouses like Snowflake or Databricks:
        -- ALTER SCHEMA {{ target_schema }} SWAP WITH {{ source_schema }};
        select 1;
    {% endset %}

    {% if execute %}
        {{ log("Executing zero-downtime blue-green schema swap...", info=True) }}
    {% endif %}
{% endmacro %}
`

const lesson57: Lesson = {
  id: 57,
  title: 'Zero-Copy Cloning & Blue-Green Deployments',
  concept: `Building models in production while analysts and dashboards are actively querying them creates race conditions, partial table reads, and query locking.

Modern enterprise data architectures implement **Blue-Green Deployments** paired with **Zero-Copy Cloning**:
1. **Clone**: Clone production schemas into a temporary staging schema (e.g. \`prod_staging\`) instantly with zero storage duplication using warehouse cloning features (\`CREATE SCHEMA prod_staging CLONE prod\`).
2. **Build**: Run \`dbt build --target staging\` entirely inside the isolated staging environment.
3. **Audit**: Run full data test suites and reconciliation checks against staging.
4. **Atomic Swap**: Execute an atomic \`ALTER SCHEMA prod SWAP WITH prod_staging\`!

The entire cutover happens in sub-second metadata time: downstream BI users never experience empty tables or inconsistent mid-flight calculations.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: orders\n`,
    'models/staging/stg_orders.sql': STG_ORDERS,
    'models/marts/fct_revenue_deploy.sql': FCT_REVENUE_DEPLOY,
    'macros/blue_green_swap.sql': MACRO_BLUE_GREEN,
  },
  seeds: {
    'raw.orders': RAW_ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_blue_green_macro',
      prompt: "Inspect `macros/blue_green_swap.sql` to understand atomic schema swapping logic.",
      hint: "Check that blue_green_swap macro is defined in macros/blue_green_swap.sql.",
      validate: (s) =>
        fileContains(s, 'macros/blue_green_swap.sql', 'blue_green_swap'),
    },
    {
      id: 'build_deploy_model',
      prompt: 'Execute `dbt build --select fct_revenue_deploy` to run the model and its tests.',
      hint: 'Run `dbt build --select fct_revenue_deploy`.',
      validate: (s) => modelRan(s, 'fct_revenue_deploy'),
    },
  ],
  quiz: {
    question: 'Why are blue-green deployments with atomic table/schema swaps preferred over rebuilding production tables directly?',
    options: [
      'They prevent downtime, locks, and partial data states for active BI dashboards while new models build',
      'They make tables impossible to delete forever',
      'They convert all data into Excel files',
      'They bypass all database access permissions',
    ],
    correctIndex: 0,
    explanation: 'By building and validating in an isolated environment first, the final production release is an instantaneous metadata swap with zero downtime for downstream consumers.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.orders', label: 'raw.orders', layer: 'source' },
        { id: 'stg_orders', label: 'stg_orders', layer: 'staging' },
        { id: 'fct_revenue_deploy', label: 'fct_revenue_deploy', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.orders', target: 'stg_orders' },
        { source: 'stg_orders', target: 'fct_revenue_deploy' },
      ],
    },
  },
  furtherReading: [
    { label: 'Zero-copy cloning in dbt', url: 'https://docs.getdbt.com/blog/snowflake-zero-copy-clone' },
  ],
}

export default lesson57
