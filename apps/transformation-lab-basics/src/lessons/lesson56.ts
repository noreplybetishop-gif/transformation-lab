import type { Lesson } from '../engine/types'
import { modelRan, lastRunSelected } from '../engine/validators'
import { RAW_CUSTOMERS_CSV } from './_canonical'

const STG_CUSTOMERS = `select
    id as customer_id,
    name,
    email,
    country
from {{ source('raw', 'customers') }}
`

const DIM_CUSTOMERS = `select
    customer_id,
    name,
    country
from {{ ref('stg_customers') }}
`

const lesson56: Lesson = {
  id: 56,
  title: 'Slim CI & State-Aware Execution',
  concept: `In enterprise dbt repositories with 1,000+ models, running \`dbt build\` on every Git Pull Request is cost-prohibitive: it can cost thousands in warehouse credits and take 45+ minutes per commit.

**Slim CI** solves this using state-aware comparison:
\`\`\`bash
dbt build --select state:modified+ --defer --state path/to/prod/manifest
\`\`\`
How it works:
1. **\`--state\`**: Compares the current git branch's code against the production \`manifest.json\` to identify *only* models whose SQL, configuration, or upstream dependencies were changed.
2. **\`state:modified+\`**: Selects modified models plus their downstream children.
3. **\`--defer\`**: When a modified downstream model references an untouched parent, dbt routes the query to the *production* table instead of forcing a rebuild of the parent in the dev environment!

In this lab, you will simulate a Slim CI workflow targeting only modified models and their downstream descendants using graph selectors.`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: customers\n`,
    'models/staging/stg_customers.sql': STG_CUSTOMERS,
    'models/marts/dim_customers.sql': DIM_CUSTOMERS,
  },
  seeds: {
    'raw.customers': RAW_CUSTOMERS_CSV,
  },
  tasks: [
    {
      id: 'simulate_edit',
      prompt: "Simulate a feature update by editing `models/staging/stg_customers.sql` to add a comment or format a column.",
      hint: "Make any small edit to stg_customers.sql.",
      validate: (s) => Boolean(s.files['models/staging/stg_customers.sql']),
    },
    {
      id: 'run_slim_ci',
      prompt: 'Execute state-targeted CI selection: `dbt build --select stg_customers+`.',
      hint: 'Type `dbt build --select stg_customers+` in the terminal.',
      validate: (s) =>
        lastRunSelected(s, ['stg_customers', 'dim_customers']) ||
        (modelRan(s, 'stg_customers') && modelRan(s, 'dim_customers')),
    },
  ],
  quiz: {
    question: 'What is the purpose of the --defer flag in a dbt Slim CI pipeline?',
    options: [
      'It delays running the command until off-peak hours',
      'It resolves references to un-modified upstream models against production instead of building them in the temporary CI schema',
      'It pauses the build if errors occur',
      'It postpones database indexing until tomorrow',
    ],
    correctIndex: 1,
    explanation: '--defer allows dbt to query existing production objects for untouched upstream dependencies, slashing CI compute costs and build duration dramatically.',
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
    { label: 'dbt Slim CI', url: 'https://docs.getdbt.com/docs/deploy/continuous-integration#slim-ci' },
  ],
}

export default lesson56
