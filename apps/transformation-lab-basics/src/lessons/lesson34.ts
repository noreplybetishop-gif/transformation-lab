import type { Lesson } from '../engine/types'
import { snapshotRan, buildSucceeded, usedFullRefresh, modelRan } from '../engine/validators'
import {
  RAW_CUSTOMERS_CSV,
  RAW_ORDERS_CSV,
  RAW_APP_EVENTS_CSV,
  RAW_SUBSCRIPTIONS_CSV,
  RAW_PAYMENTS_CSV,
  COUNTRIES_CSV,
} from './_canonical'

const DBT_PROJECT_YML = `name: transformation_lab
version: "1.0.0"
config-version: 2
profile: transformation_lab

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

models:
  transformation_lab:
    staging:
      +materialized: view
    marts:
      +materialized: table
`

const SOURCES_YML = `version: 2

sources:
  - name: raw
    tables:
      - name: customers
      - name: orders
      - name: app_events
      - name: subscriptions
      - name: payments
`

const SNAP_SUBSCRIPTIONS = `{% snapshot snap_customer_subscriptions %}

{{
    config(
      target_schema='main',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('raw', 'subscriptions') }}

{% endsnapshot %}
`

const MACRO_CENTS_TO_DOLLARS = `{% macro cents_to_dollars(column_name) %}
    round(({{ column_name }} / 100.0), 2)
{% endmacro %}
`

const MACRO_TEST_IS_POSITIVE = `{% test is_positive(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}
`

const STG_CUSTOMERS = `select
    id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}
`

const STG_ORDERS = `select
    id as order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ source('raw', 'orders') }}
`

const STG_PAYMENTS = `select
    id as payment_id,
    order_id,
    payment_method,
    {{ cents_to_dollars('amount_cents') }} as amount
from {{ source('raw', 'payments') }}
`

const STG_APP_EVENTS = `select
    event_id,
    user_id,
    event_name,
    event_timestamp
from {{ source('raw', 'app_events') }}
`

const FCT_APP_EVENTS = `{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

select
    event_id,
    user_id,
    event_name,
    event_name = 'checkout' as is_checkout,
    event_timestamp
from {{ ref('stg_app_events') }}

{% if is_incremental() %}
  where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}
`

const DIM_CUSTOMERS = `select
    id as customer_id,
    name,
    email,
    country
from {{ ref('stg_customers') }}
`

const DIM_CURRENT_SUBSCRIPTIONS = `select
    id as subscription_id,
    customer_id,
    plan,
    status
from {{ ref('snap_customer_subscriptions') }}
where dbt_valid_to is null
`

const SCHEMA_YML = `version: 2

models:
  - name: stg_payments
    columns:
      - name: payment_id
        data_tests:
          - not_null
          - unique
      - name: amount
        data_tests:
          - not_null
          - is_positive
  - name: fct_app_events
    columns:
      - name: event_id
        data_tests:
          - not_null
          - unique
`

const lesson34: Lesson = {
  id: 34,
  title: 'The Enterprise Pipeline Capstone',
  concept: `Congratulations on reaching the **Intermediate Capstone**!

Take a look around your project workspace. This project embodies the full spectrum of intermediate analytics engineering patterns:

1. **Incremental Processing**: \`fct_app_events\` builds incrementally with \`is_incremental()\`, prevents duplicate rows via \`unique_key='event_id'\`, and supports \`--full-refresh\` rebuilds.
2. **Slowly Changing Dimensions (SCD Type 2)**: \`snap_customer_subscriptions\` tracks historical attribute changes over time, while \`dim_current_subscriptions\` exposes current state with \`where dbt_valid_to is null\`.
3. **Modular Jinja & Macros**: Reusable calculations (\`cents_to_dollars\`), parameterized macros, dynamic loops, and custom generic data tests (\`is_positive\`).
4. **Project Configuration**: Hierarchical folder defaults in \`dbt_project.yml\`, project variables with CLI overrides, custom schema routing, and contracts.

In this capstone lab, you will orchestrate the entire enterprise pipeline:
- Run \`dbt snapshot\` to capture historical dimension state.
- Run \`dbt build\` to build and test the full DAG in dependency order.
- Rebuild the incremental event mart with \`--full-refresh\`.`,
  initialFiles: {
    'dbt_project.yml': DBT_PROJECT_YML,
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/_schema.yml': SCHEMA_YML,
    'snapshots/snap_customer_subscriptions.sql': SNAP_SUBSCRIPTIONS,
    'macros/cents_to_dollars.sql': MACRO_CENTS_TO_DOLLARS,
    'macros/test_is_positive.sql': MACRO_TEST_IS_POSITIVE,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
    'models/staging/stg_orders.sql': STG_ORDERS,
    'models/staging/stg_payments.sql': STG_PAYMENTS,
    'models/staging/stg_app_events.sql': STG_APP_EVENTS,
    'models/marts/dim_customers.sql': DIM_CUSTOMERS,
    'models/marts/dim_current_subscriptions.sql': DIM_CURRENT_SUBSCRIPTIONS,
    'models/marts/fct_app_events.sql': FCT_APP_EVENTS,
    'seeds/countries.csv': COUNTRIES_CSV,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
    'raw.orders': RAW_ORDERS_CSV,
    'raw.app_events': RAW_APP_EVENTS_CSV,
    'raw.subscriptions': RAW_SUBSCRIPTIONS_CSV,
    'raw.payments': RAW_PAYMENTS_CSV,
  },
  tasks: [
    {
      id: 'snapshot',
      prompt: 'Execute `dbt snapshot` to record dimension state across the project.',
      hint: 'Type `dbt snapshot` and press Enter.',
      validate: (s) => snapshotRan(s) || s.lastRun?.command === 'snapshot',
    },
    {
      id: 'build_all',
      prompt: 'Execute `dbt build` to build every seed, model, and test in DAG order.',
      hint: 'Run `dbt build` in the terminal.',
      validate: (s) => buildSucceeded(s) && s.lastRun?.command === 'build',
    },
    {
      id: 'full_refresh',
      prompt: 'Perform a full refresh on the incremental event mart: `dbt run --full-refresh --select fct_app_events`.',
      hint: 'Run `dbt run --full-refresh --select fct_app_events`.',
      validate: (s) => usedFullRefresh(s) && modelRan(s, 'fct_app_events'),
    },
  ],
  quiz: {
    question: 'Which of the following describes the complete enterprise workflow when deploying new dbt code?',
    options: [
      'Only run dbt test in production without building models',
      'Run snapshots, build models and tests in DAG order with dbt build, and use --full-refresh when schemas change',
      'Delete the warehouse database before every run',
      'Manually copy tables between dev and prod environments',
    ],
    correctIndex: 1,
    explanation: 'A production dbt pipeline updates snapshots, runs `dbt build` to materialize models and validate tests in topological order, and leverages `--full-refresh` when incremental models require backfills or schema alterations.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'raw.orders', label: 'raw.orders', layer: 'source' },
        { id: 'raw.app_events', label: 'raw.app_events', layer: 'source' },
        { id: 'raw.subscriptions', label: 'raw.subscriptions', layer: 'source' },
        { id: 'raw.payments', label: 'raw.payments', layer: 'source' },
        { id: 'stg_customers', label: 'stg_customers', layer: 'staging' },
        { id: 'stg_orders', label: 'stg_orders', layer: 'staging' },
        { id: 'stg_payments', label: 'stg_payments', layer: 'staging' },
        { id: 'stg_app_events', label: 'stg_app_events', layer: 'staging' },
        { id: 'snap_customer_subscriptions', label: 'snap_customer_subscriptions', layer: 'intermediate' },
        { id: 'dim_customers', label: 'dim_customers', layer: 'mart' },
        { id: 'dim_current_subscriptions', label: 'dim_current_subscriptions', layer: 'mart' },
        { id: 'fct_app_events', label: 'fct_app_events', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.customers', target: 'stg_customers' },
        { source: 'raw.orders', target: 'stg_orders' },
        { source: 'raw.payments', target: 'stg_payments' },
        { source: 'raw.app_events', target: 'stg_app_events' },
        { source: 'raw.subscriptions', target: 'snap_customer_subscriptions' },
        { source: 'stg_customers', target: 'dim_customers' },
        { source: 'stg_app_events', target: 'fct_app_events' },
        { source: 'snap_customer_subscriptions', target: 'dim_current_subscriptions' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt build command', url: 'https://docs.getdbt.com/reference/commands/build' },
    { label: 'Continuous Integration with dbt', url: 'https://docs.getdbt.com/docs/deploy/continuous-integration' },
  ],
}

export default lesson34
