import type { Lesson } from '../engine/types'
import { commandRan, fileContains } from '../engine/validators'
import { RAW_ORDERS_CSV } from './_canonical'

const STG_ORDERS = `select
    id as order_id,
    customer_id,
    amount,
    status,
    case
        when amount >= 50.00 then 'high'
        when amount >= 20.00 then 'medium'
        else 'low'
    end as order_tier
from {{ source('raw', 'orders') }}
`

const UNIT_TEST_YML = `version: 2

models:
  - name: stg_orders
    unit_tests:
      - name: test_order_tier_logic
        description: "Verify tier classification boundaries without querying production data"
        model: stg_orders
        given:
          - input: source('raw', 'orders')
            rows:
              - { id: 1, customer_id: 1, amount: 60.00, status: 'paid', created_at: '2024-01-01' }
              - { id: 2, customer_id: 1, amount: 25.00, status: 'paid', created_at: '2024-01-01' }
              - { id: 3, customer_id: 1, amount: 10.00, status: 'paid', created_at: '2024-01-01' }
        expect:
          rows:
            - { order_id: 1, order_tier: 'high' }
            - { order_id: 2, order_tier: 'medium' }
            - { order_id: 3, order_tier: 'low' }
`

const lesson44: Lesson = {
  id: 44,
  title: 'Native dbt Unit Testing',
  concept: `Data tests validate live data in your warehouse. **Unit tests** validate your SQL *logic* in isolation before code ever reaches production!

Introduced natively in modern dbt (1.8+), \`unit_tests:\` allow analytics engineers to mock input rows in YAML and define the exact expected output rows:
\`\`\`yaml
unit_tests:
  - name: test_order_tier_logic
    model: stg_orders
    given:
      - input: source('raw', 'orders')
        rows:
          - { id: 1, amount: 75.00 }
    expect:
      rows:
        - { order_id: 1, order_tier: 'high' }
\`\`\`
dbt compiles a synthetic CTE substituting the mocked inputs, executes the model logic against the mock, and asserts equality against \`expect\`. This allows true Test-Driven Development (TDD) for data transformations!`,
  initialFiles: {
    'models/staging/_sources.yml': `version: 2\nsources:\n  - name: raw\n    tables:\n      - name: orders\n`,
    'models/staging/stg_orders.sql': STG_ORDERS,
    'models/staging/_unit_tests.yml': UNIT_TEST_YML,
  },
  seeds: {
    'raw.orders': RAW_ORDERS_CSV,
  },
  tasks: [
    {
      id: 'verify_unit_test',
      prompt: "Inspect `models/staging/_unit_tests.yml` and ensure `unit_tests:` is defined for `stg_orders`.",
      hint: "Check that test_order_tier_logic has given inputs and expect rows configured.",
      validate: (s) =>
        fileContains(s, 'models/staging/_unit_tests.yml', 'unit_tests') &&
        fileContains(s, 'models/staging/_unit_tests.yml', 'test_order_tier_logic'),
    },
    {
      id: 'run_unit_test',
      prompt: 'Execute `dbt test` to run the model unit test.',
      hint: 'Run `dbt test` in the terminal.',
      validate: (s) => commandRan(s, 'test') || commandRan(s, 'build'),
    },
  ],
  quiz: {
    question: 'How do dbt unit tests differ fundamentally from traditional data tests?',
    options: [
      'Unit tests only test database indexes',
      'Unit tests validate business logic against isolated mock data defined in YAML rather than querying real warehouse tables',
      'Unit tests require writing Python scripts',
      'Unit tests can only run in production deployments',
    ],
    correctIndex: 1,
    explanation: 'Unit tests decouple logic verification from production data by supplying synthetic input rows and asserting expected output structures, enabling rapid TDD and edge-case testing.',
  },
  goal: {
    dagShape: {
      nodes: [
        { id: 'raw.orders', label: 'raw.orders', layer: 'source' },
        { id: 'stg_orders', label: 'stg_orders', layer: 'staging' },
      ],
      edges: [
        { source: 'raw.orders', target: 'stg_orders' },
      ],
    },
  },
  furtherReading: [
    { label: 'dbt Unit Testing', url: 'https://docs.getdbt.com/docs/build/unit-tests' },
  ],
}

export default lesson44
