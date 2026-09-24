import type { Lesson } from '../engine/types'
import { fileMatches } from '../engine/validators'
import { RAW_CUSTOMERS_CSV } from './_canonical'

const SOURCES_YML = `version: 2

sources:
  - name: raw
    tables:
      - name: customers
`

const STG_CUSTOMERS = `select
    id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}
`

const DIM_CUSTOMERS = `select
    id as customer_id,
    name,
    email,
    country
from {{ ref('stg_customers') }}
`

const EXPOSURES_YML = `version: 2

exposures:
  - name: executive_kpi_dashboard
    type: dashboard
    maturity: high
    url: https://bi.company.com/dashboards/1
    description: "Weekly executive revenue & customer health dashboard."
    owner:
      name: Analytics Team
      email: data@example.com
    depends_on:
      - ref('dim_customers')
`

const lesson33: Lesson = {
  id: 33,
  title: 'Exposures & Downstream Lineage',
  concept: `Your data pipeline doesn't end when a dbt model is materialized. In the real world, models are consumed by downstream assets:
- **Dashboards**: Tableau, Looker, Metabase, PowerBI reports.
- **Machine Learning Pipelines**: Churn predictors, fraud classifiers.
- **Reverse ETL**: Syncing user segments to HubSpot, Salesforce, or Braze.

When an engineer needs to modify or deprecate a column, how do they know which critical dashboard will break?

dbt solves this with **Exposures**: defining downstream consumers directly in your dbt documentation and DAG!

Exposures are declared in YAML:

\`\`\`yaml
version: 2

exposures:
  - name: executive_kpi_dashboard
    type: dashboard
    maturity: high
    url: https://bi.company.com/dashboards/1
    description: "Weekly executive revenue & customer health dashboard."
    owner:
      name: Analytics Team
      email: data@example.com
    depends_on:
      - ref('dim_customers')
\`\`\`

With exposures defined:
1. Lineage graphs show the complete journey from raw source data all the way to the executive dashboard.
2. You can target tests to only the models that power an exposure:
   \`dbt test --select +exposure:executive_kpi_dashboard\`

In this lab, inspect \`models/exposures.yml\` and compile the project to integrate the exposure into the manifest.`,
  initialFiles: {
    'models/staging/_sources.yml': SOURCES_YML,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
    'models/marts/dim_customers.sql': DIM_CUSTOMERS,
    'models/exposures.yml': EXPOSURES_YML,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  openFiles: ['models/exposures.yml', 'models/marts/dim_customers.sql'],
  tasks: [
    {
      id: 'verify_exposure',
      prompt: "Verify `models/exposures.yml` declares `executive_kpi_dashboard` with `depends_on: - ref('dim_customers')`.",
      hint: "Check `models/exposures.yml` for `name: executive_kpi_dashboard` and `ref('dim_customers')` under `depends_on`.",
      validate: (s) =>
        fileMatches(s, 'models/exposures.yml', /name:\s*executive_kpi_dashboard/i) &&
        fileMatches(s, 'models/exposures.yml', /ref\s*\(\s*['"]dim_customers['"]\s*\)/i),
    },
    {
      id: 'compile',
      prompt: 'Run `dbt compile` to parse and integrate the exposure into the DAG.',
      hint: 'Type `dbt compile` in the terminal.',
      validate: (s) => s.lastRun?.command === 'compile',
    },
  ],
  quiz: {
    question: 'What is the primary benefit of declaring exposures in a dbt project?',
    options: [
      'It automatically exports data to Google Sheets every hour',
      'It connects downstream BI dashboards and ML pipelines into the DAG for impact analysis and targeted testing',
      'It encrypts sensitive columns for GDPR compliance',
      'It replaces SQL models with Python scripts',
    ],
    correctIndex: 1,
    explanation: 'Exposures document and connect downstream consumers (BI, reverse ETL, ML) to upstream dbt models, providing complete end-to-end lineage and impact analysis.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.customers', label: 'raw.customers', layer: 'source' },
        { id: 'stg_customers', label: 'stg_customers', layer: 'staging' },
        { id: 'dim_customers', label: 'dim_customers', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.customers', target: 'stg_customers' },
        { source: 'stg_customers', target: 'dim_customers' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Exposures', url: 'https://docs.getdbt.com/docs/build/exposures' },
    { label: 'Selecting exposures in the CLI', url: 'https://docs.getdbt.com/reference/node-selection/syntax#exposures' },
  ],
}

export default lesson33
