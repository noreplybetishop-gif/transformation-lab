import type { Lesson } from '../engine/types'
import { commandRan, fileContains } from '../engine/validators'
import { RAW_ORDERS_CSV } from './_canonical'

const STG_ORDERS = `select
    id as order_id,
    customer_id,
    amount,
    status,
    created_at
from {{ source('raw', 'orders') }}
`

const FCT_REVENUE = `select
    customer_id,
    sum(amount) as total_revenue
from {{ ref('stg_orders') }}
where status = 'paid'
group by customer_id
`

const EXPOSURES_YML = `version: 2

exposures:
  - name: executive_revenue_dashboard
    label: "Executive Revenue KPI Dashboard"
    type: dashboard
    maturity: high
    url: https://bi.company.com/dashboards/exec-revenue
    description: "Daily revenue numbers presented to C-suite and leadership"
    depends_on:
      - ref('fct_revenue')
    owner:
      name: BI Engineering Team
      email: bi-team@example.com
`

const lesson50: Lesson = {
  id: 50,
  title: 'Exposures & Downstream BI Lineage',
  concept: `Where does data go after dbt builds your models? Usually into executive dashboards, automated ML scoring jobs, or reverse-ETL syncs into Salesforce.

Without visibility into these consumers, engineers refactoring a model have no idea which dashboards they might break.

**dbt Exposures** define downstream consumption directly in the DAG:
\`\`\`yaml
exposures:
  - name: executive_revenue_dashboard
    type: dashboard
    maturity: high
    depends_on:
      - ref('fct_revenue')
    owner:
      name: BI Team
      email: bi@example.com
\`\`\`
Exposures appear directly in your dbt lineage graph! When testing or refactoring upstream models, running:
\`dbt test --select +exposure:executive_revenue_dashboard\`
tests all upstream models feeding that critical executive dashboard!`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: orders\n`,
    'models/staging/stg_orders.sql': STG_ORDERS,
    'models/marts/fct_revenue.sql': FCT_REVENUE,
    'models/exposures.yml': EXPOSURES_YML,
  },
  seeds: {
    'raw.orders': RAW_ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_exposure',
      prompt: "Inspect `models/exposures.yml` to verify that `exposures:` and `depends_on:` referencing `fct_revenue` are configured.",
      hint: "Check that executive_revenue_dashboard lists ref('fct_revenue') under depends_on.",
      validate: (s) =>
        fileContains(s, 'models/exposures.yml', 'exposures') &&
        fileContains(s, 'models/exposures.yml', "ref('fct_revenue')"),
    },
    {
      id: 'compile_dag_with_exposure',
      prompt: 'Execute `dbt compile` to incorporate the exposure into the project manifest.',
      hint: 'Type `dbt compile` in the console.',
      validate: (s) => commandRan(s, 'compile') || commandRan(s, 'build'),
    },
  ],
  quiz: {
    question: 'How do dbt exposures help prevent breaking changes in production?',
    options: [
      'They prevent users from logging into BI dashboards',
      'They connect downstream dashboards and applications to the dbt DAG, enabling impact analysis before making model changes',
      'They encrypt the database credentials',
      'They automatically rebuild the warehouse hardware',
    ],
    correctIndex: 1,
    explanation: 'Exposures document downstream consumers in code and in the lineage graph, allowing data engineers to run impact analysis (e.g. +exposure:dashboard) before modifying upstream tables.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.orders', label: 'raw.orders', layer: 'source' },
        { id: 'stg_orders', label: 'stg_orders', layer: 'staging' },
        { id: 'fct_revenue', label: 'fct_revenue', layer: 'mart' },
      ],
      edges: [
        { source: 'raw.orders', target: 'stg_orders' },
        { source: 'stg_orders', target: 'fct_revenue' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Exposures', url: 'https://docs.getdbt.com/docs/build/exposures' },
  ],
}

export default lesson50
