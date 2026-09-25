import type { Lesson } from '../engine/types'
import { snapshotRan, buildSucceeded, commandRan } from '../engine/validators'
import {
  RAW_CUSTOMERS_CSV,
  RAW_ORDERS_CSV,
  RAW_APP_EVENTS_CSV,
  RAW_SUBSCRIPTIONS_CSV,
  RAW_PAYMENTS_CSV,
  COUNTRIES_CSV,
} from './_canonical'

const DBT_PROJECT_YML = `name: transformation_lab
version: "2.0.0"
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
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 72, period: hour}
    loaded_at_field: created_at
    tables:
      - name: customers
      - name: orders
      - name: app_events
      - name: subscriptions
      - name: payments
`

const SNAP_SUBSCRIPTIONS = `{% snapshot snap_enterprise_subscriptions %}

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

const MACRO_IS_POSITIVE = `{% test is_positive(model, column_name) %}

select
    {{ column_name }} as invalid_value
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}
`

const STG_CUSTOMERS = `select
    id as customer_id,
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
    amount_cents / 100.0 as amount
from {{ source('raw', 'payments') }}
`

const DIM_CUSTOMERS = `select
    customer_id,
    name,
    email,
    country
from {{ ref('stg_customers') }}
`

const FCT_ORDERS_INCREMENTAL = `{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

select
    order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ ref('stg_orders') }}
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
  - name: fct_orders_incremental
    columns:
      - name: order_id
        data_tests:
          - not_null
          - unique
`

const EXPOSURES_YML = `version: 2

exposures:
  - name: executive_financial_report
    type: dashboard
    maturity: high
    depends_on:
      - ref('fct_orders_incremental')
    owner:
      name: Enterprise Data Platform Team
      email: data-platform@example.com
`

const lesson59: Lesson = {
  id: 59,
  title: 'The Advanced Platform Capstone',
  concept: `**Welcome to the Advanced Capstone!** You have traversed the complete software engineering paradigm for modern data transformation.

Your project in this capstone showcases an end-to-end production data platform:
1. **Incremental Merge Engines**: High-performance \`MERGE INTO\` tables with unique keys.
2. **SCD Type 2 Snapshots**: Immutable historical tracking for dimension lifecycles.
3. **Custom Generic Tests**: Reusable assertions (\`is_positive\`) validating numerical integrity.
4. **Source SLAs & Lineage Exposures**: Freshness thresholds paired with downstream executive reporting contracts.

In this final lab, you will orchestrate the full enterprise release lifecycle:
- Execute \`dbt snapshot\` to record historical dimensions.
- Execute \`dbt test\` to validate all schema and custom data tests.
- Execute \`dbt build\` to compile and materialize the entire verified DAG!`,
  initialFiles: {
    'dbt_project.yml': DBT_PROJECT_YML,
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/_schema.yml': SCHEMA_YML,
    'models/exposures.yml': EXPOSURES_YML,
    'snapshots/snap_enterprise_subscriptions.sql': SNAP_SUBSCRIPTIONS,
    'macros/test_is_positive.sql': MACRO_IS_POSITIVE,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
    'models/staging/stg_orders.sql': STG_ORDERS,
    'models/staging/stg_payments.sql': STG_PAYMENTS,
    'models/marts/dim_customers.sql': DIM_CUSTOMERS,
    'models/marts/fct_orders_incremental.sql': FCT_ORDERS_INCREMENTAL,
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
      id: 'snapshot_dimensions',
      prompt: 'Execute `dbt snapshot` to update dimension state and record historical SCD2 validity intervals.',
      hint: 'Type `dbt snapshot` in the console.',
      validate: (s) => snapshotRan(s) || s.lastRun?.command === 'snapshot',
    },
    {
      id: 'validate_tests',
      prompt: 'Execute `dbt test` to validate all data quality assertions and custom generic tests.',
      hint: 'Run `dbt test` in the terminal.',
      validate: (s) => commandRan(s, 'test') || commandRan(s, 'build'),
    },
    {
      id: 'build_platform',
      prompt: 'Execute `dbt build` to build every seed, model, test, and snapshot in dependency order.',
      hint: 'Run `dbt build` in the terminal.',
      validate: (s) => buildSucceeded(s) && s.lastRun?.command === 'build',
    },
  ],
  quiz: {
    question: 'Which of the following describes the most robust architecture for mission-critical enterprise dbt deployments?',
    options: [
      'Only running models when errors occur in production',
      'Combining unit tests, enforced contracts, automated CI with --defer, blue-green atomic deployments, and full dbt build orchestration',
      'Manually writing CSV files into the warehouse',
      'Disabling tests to ensure builds run faster',
    ],
    correctIndex: 1,
    explanation: 'A production-grade enterprise data platform leverages unit tests during development, enforced model contracts, state-aware Slim CI, blue-green deployments, and end-to-end dbt build validation.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'raw.orders', label: 'raw.orders', layer: 'source' },
        { id: 'raw.payments', label: 'raw.payments', layer: 'source' },
        { id: 'raw.subscriptions', label: 'raw.subscriptions', layer: 'source' },
        { id: 'stg_customers', label: 'stg_customers', layer: 'staging' },
        { id: 'stg_orders', label: 'stg_orders', layer: 'staging' },
        { id: 'stg_payments', label: 'stg_payments', layer: 'staging' },
        { id: 'snap_enterprise_subscriptions', label: 'snap_enterprise_subscriptions', layer: 'intermediate' },
        { id: 'dim_customers', label: 'dim_customers', layer: 'mart' },
        { id: 'fct_orders_incremental', label: 'fct_orders_incremental', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.customers', target: 'stg_customers' },
        { source: 'raw.orders', target: 'stg_orders' },
        { source: 'raw.payments', target: 'stg_payments' },
        { source: 'raw.subscriptions', target: 'snap_enterprise_subscriptions' },
        { source: 'stg_customers', target: 'dim_customers' },
        { source: 'stg_orders', target: 'fct_orders_incremental' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Best Practices', url: 'https://docs.getdbt.com/best-practices' },
    { label: 'Enterprise dbt deployment guide', url: 'https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview' },
  ],
}

export default lesson59
