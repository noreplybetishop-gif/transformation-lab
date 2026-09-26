import { useState } from 'react'
import { useGameStore, lessonCompleted } from '../store/gameStore'

type SparkStageId = 'basics' | 'intermediate' | 'advanced'

interface StageInfo {
  id: SparkStageId
  name: string
  subtitle: string
  labCount: number
  summary: string
  highlights: string[]
  topics: { title: string; desc: string }[]
}

const SPARK_STAGES: StageInfo[] = [
  {
    id: 'basics',
    name: 'Basics',
    subtitle: 'Foundations of Distributed Compute',
    labCount: 12,
    summary:
      'Master the core architecture of Apache Spark: the Driver, Cluster Manager, and Executors. Understand lazy evaluation, Resilient Distributed Datasets (RDDs), DataFrame transformations vs actions, and Spark SQL query syntax.',
    highlights: [
      'SparkSession, Driver & Executor topology',
      'Narrow vs Wide transformations',
      'DataFrame schemas, types & projection',
      'Lazy DAG execution & Action triggers (count, collect, show)',
      'Basic aggregations & Spark SQL queries',
    ],
    topics: [
      { title: 'Lab 1 · The Spark Driver & SparkSession', desc: 'Initialize SparkSession and inspect cluster active nodes and memory configurations.' },
      { title: 'Lab 2 · RDD Fundamentals & Immutability', desc: 'Create Resilient Distributed Datasets, apply map/filter, and trace lineage partitions.' },
      { title: 'Lab 3 · DataFrames vs RDDs', desc: 'Load structured datasets into DataFrames with schema inference and catalog metadata.' },
      { title: 'Lab 4 · Narrow vs Wide Transformations', desc: 'Compare pipelined narrow dependencies (map, filter) against stage-boundary wide shuffles.' },
      { title: 'Lab 5 · Lazy Evaluation in Practice', desc: 'Observe how Spark builds an execution DAG without evaluating until an Action triggers.' },
      { title: 'Lab 6 · Common Actions: count, collect & take', desc: 'Retrieve distributed results safely to the driver without triggering Out-Of-Memory errors.' },
      { title: 'Lab 7 · Spark SQL & Temp Views', desc: 'Register temporary catalog views (createOrReplaceTempView) and execute ANSI SQL queries.' },
      { title: 'Lab 8 · Column Expressions & withColumn', desc: 'Add computed columns, perform type casting, and implement conditional when/otherwise logic.' },
      { title: 'Lab 9 · Grouping & Aggregate Functions', desc: 'Compute distributed metrics with groupBy, sum, avg, and agg functions.' },
      { title: 'Lab 10 · Handling Nulls & Data Cleaning', desc: 'Clean dirty partitions with dropna, fillna, and coalesce functions.' },
      { title: 'Lab 11 · Reading & Writing CSV & JSON', desc: 'Read multi-line JSON and CSV sources with custom schemas and write partitioned outputs.' },
      { title: 'Lab 12 · Basics Capstone: Batch Aggregator', desc: 'Orchestrate an end-to-end batch ingestion and cleaning pipeline in Spark.' },
    ],
  },
  {
    id: 'intermediate',
    name: 'Intermediate',
    subtitle: 'PySpark & Lakehouse Processing',
    labCount: 15,
    summary:
      'Build robust, production-grade PySpark pipelines. Master analytical window functions, complex nested structures, broadcast hash joins, caching memory levels, partition pruning, and writing to Parquet & Delta Lake.',
    highlights: [
      'PySpark DataFrame API & Custom Python UDFs',
      'Windowing functions (row_number, lead/lag, rolling averages)',
      'Join algorithms: Broadcast Hash vs Sort-Merge',
      'Storage levels: cache() vs persist(StorageLevel.MEMORY_AND_DISK)',
      'Delta Lake ACID transactions & time travel',
    ],
    topics: [
      { title: 'Lab 13 · PySpark Functional Syntax & Method Chaining', desc: 'Write clean, maintainable Python transformation pipelines using chaining patterns.' },
      { title: 'Lab 14 · Advanced Analytical Window Functions', desc: 'Partition by entity and order by timestamp to compute running totals, ranks, and lead/lag intervals.' },
      { title: 'Lab 15 · Handling Arrays, Structs & Maps', desc: 'Manipulate nested JSON payloads using explode, struct, and array_contains functions.' },
      { title: 'Lab 16 · Join Strategies: Broadcast Hash Joins', desc: 'Use broadcast() hints to eliminate shuffles when joining large facts with small dimension tables.' },
      { title: 'Lab 17 · Sort-Merge Joins & Key Partitioning', desc: 'Analyze how Spark co-locates and sorts matching join keys across worker partitions.' },
      { title: 'Lab 18 · Caching Strategies: cache() vs persist()', desc: 'Prevent redundant DAG recalculations with in-memory storage levels and off-heap options.' },
      { title: 'Lab 19 · Partitioning & Bucketing for Queries', desc: 'Optimize storage layouts with partitionBy and bucketBy to prune read scans.' },
      { title: 'Lab 20 · User-Defined Functions (UDFs) & Pandas UDFs', desc: 'Implement vectorized Apache Arrow-powered Pandas UDFs for maximum performance.' },
      { title: 'Lab 21 · Writing Efficient Parquet Files', desc: 'Control file sizes, compression codecs (Snappy, Zstandard), and avoid the small-files problem.' },
      { title: 'Lab 22 · Delta Lake ACID Transactions', desc: 'Build Lakehouse tables with atomic commits, merge into (upsert), and schema evolution.' },
      { title: 'Lab 23 · Time Travel & Table History in Delta', desc: 'Query historical table snapshots using versionAsOf and timestampAsOf for regression testing.' },
      { title: 'Lab 24 · Repartition vs Coalesce', desc: 'Rebalance partition distributions (repartition) or consolidate partitions without a shuffle (coalesce).' },
      { title: 'Lab 25 · Broadcast Variables & Accumulators', desc: 'Share read-only lookup dictionaries and aggregate distributed audit counters across executors.' },
      { title: 'Lab 26 · Multi-Dataset Joins & Complex Lineages', desc: 'Resolve multi-way joins with aliases and avoid column ambiguity in complex graphs.' },
      { title: 'Lab 27 · Intermediate Capstone: E-Commerce Lakehouse', desc: 'Build an end-to-end medallion architecture (Bronze -> Silver -> Gold) with Delta Lake.' },
    ],
  },
  {
    id: 'advanced',
    name: 'Advanced',
    subtitle: 'Performance, Catalyst & Streaming',
    labCount: 18,
    summary:
      'Scale Spark to petabytes and multi-thousand core clusters. Deep-dive into Catalyst query plan optimization, Adaptive Query Execution (AQE), dynamic partition pruning, shuffle skew resolution, memory spill debugging, and Structured Streaming.',
    highlights: [
      'Catalyst Optimizer: Parsed, Analyzed, Logical & Physical Plans',
      'Adaptive Query Execution (AQE): Dynamic coalescing & skew joins',
      'Shuffle tuning & resolving Executor OutOfMemory (OOM)',
      'Memory management: Execution vs Storage memory pools',
      'Structured Streaming with Event-Time Watermarking',
    ],
    topics: [
      { title: 'Lab 28 · The Catalyst Optimizer Internals', desc: 'Deconstruct query plans using df.explain(extended=True) from AST to code-generated physical plans.' },
      { title: 'Lab 29 · Whole-Stage Code Generation (Tungsten)', desc: 'Explore JVM bytecode generation and CPU cache-aware memory layouts under Project Tungsten.' },
      { title: 'Lab 30 · Adaptive Query Execution (AQE) Deep Dive', desc: 'Configure spark.sql.adaptive.enabled to dynamically adjust shuffle partitions at runtime.' },
      { title: 'Lab 31 · Dynamic Partition Pruning (DPP)', desc: 'Understand how dimension filter results are broadcasted to prune fact table partitions dynamically.' },
      { title: 'Lab 32 · Data Skew Detection & Salt-Key Mitigation', desc: 'Identify unbalanced partition bottlenecks in the Spark UI and apply key salting to redistribute load.' },
      { title: 'Lab 33 · Memory Management & Spill to Disk', desc: 'Diagnose memory allocation between Execution and Storage pools, tuning spark.memory.fraction.' },
      { title: 'Lab 34 · Driver & Executor Sizing for Production', desc: 'Calculate optimal executor cores (4-5), executor memory, and overheads for cluster deployment.' },
      { title: 'Lab 35 · Diagnosing the Spark UI & Event Logs', desc: 'Analyze Stage timelines, task metrics, GC pauses, and shuffle read/write skews like a staff engineer.' },
      { title: 'Lab 36 · Structured Streaming Fundamentals', desc: 'Read real-time continuous streams with readStream and writeStream to console and memory sinks.' },
      { title: 'Lab 37 · Event-Time Processing & Watermarking', desc: 'Handle late-arriving records with withWatermark() to close window aggregation states safely.' },
      { title: 'Lab 38 · Stream-Static & Stream-Stream Joins', desc: 'Join real-time streams against dimension tables and correlate dual streaming event topics.' },
      { title: 'Lab 39 · Deduplication & Idempotent Stream Sinks', desc: 'Prevent duplicate events in streaming pipelines with dropDuplicates and checkpointing.' },
      { title: 'Lab 40 · Delta Lake Streaming & Change Data Capture (CDC)', desc: 'Ingest Kafka event streams directly into Delta Lake tables with ACID guarantees.' },
      { title: 'Lab 41 · Cost Optimization on Cloud Spark (EMR / Databricks)', desc: 'Configure spot instances, cluster auto-scaling, and autoscaling local storage for big savings.' },
      { title: 'Lab 42 · Custom Partitioners & Salting Strategies', desc: 'Implement custom hash and range partitioners to optimize downstream grouping.' },
      { title: 'Lab 43 · JVM Garbage Collection Tuning for Spark', desc: 'Configure G1GC parameters (-XX:+UseG1GC, InitiatingHeapOccupancyPercent) to eliminate stop-the-world pauses.' },
      { title: 'Lab 44 · Data Governance & Lineage in Modern Lakehouses', desc: 'Track column-level lineage and integrate Spark pipelines with catalog tools like Unity Catalog.' },
      { title: 'Lab 45 · Advanced Capstone: Real-Time Financial Fraud Engine', desc: 'Deploy an enterprise streaming & batch anomaly detection platform scaling across millions of events.' },
    ],
  },
]

export default function SparkPage() {
  const [selectedStage, setSelectedStage] = useState<SparkStageId | null>(null)
  const [emailInput, setEmailInput] = useState('')
  const [subscribed, setSubscribed] = useState(false)
  const completedTasks = useGameStore((s) => s.completedTasks)
  const loadLesson = useGameStore((s) => s.loadLesson)

  // Basics: Labs 101 to 112
  const basicsIds = Array.from({ length: 12 }, (_, i) => 101 + i)
  const completedBasicsCount = basicsIds.filter((id) => lessonCompleted(completedTasks, id)).length
  const hasBasicsProgress = completedBasicsCount > 0
  const resumeBasicsId = basicsIds.find((id) => !lessonCompleted(completedTasks, id)) ?? 101

  // Intermediate: Labs 113 to 127
  const interIds = Array.from({ length: 15 }, (_, i) => 113 + i)
  const completedInterCount = interIds.filter((id) => lessonCompleted(completedTasks, id)).length
  const hasInterProgress = completedInterCount > 0
  const resumeInterId = interIds.find((id) => !lessonCompleted(completedTasks, id)) ?? 113

  // Advanced: Labs 128 to 145
  const advIds = Array.from({ length: 18 }, (_, i) => 128 + i)
  const completedAdvCount = advIds.filter((id) => lessonCompleted(completedTasks, id)).length
  const hasAdvProgress = completedAdvCount > 0
  const resumeAdvId = advIds.find((id) => !lessonCompleted(completedTasks, id)) ?? 128

  const hasAnySparkProgress = hasBasicsProgress || hasInterProgress || hasAdvProgress

  const activeStage = selectedStage ? SPARK_STAGES.find((s) => s.id === selectedStage) : null

  const handleNavigateToDbt = (lessonId: number = 0) => {
    void loadLesson(lessonId)
    const target = lessonId === 0 ? '/' : `/lesson/${lessonId}`
    window.history.pushState(null, '', target)
    window.dispatchEvent(new PopStateEvent('popstate'))
  }

  const handleLaunchLab = (lessonId: number) => {
    void loadLesson(lessonId)
    const target = `/spark/lesson/${lessonId}`
    window.history.pushState(null, '', target)
    window.dispatchEvent(new PopStateEvent('popstate'))
  }

  const handleSubscribe = (e: React.FormEvent) => {
    e.preventDefault()
    if (emailInput.trim()) {
      setSubscribed(true)
      try {
        localStorage.setItem('transformation-lab-spark-waitlist', emailInput.trim())
      } catch {
        /* ignore */
      }
    }
  }

  return (
    <div
      className="flex-1 overflow-y-auto"
      style={{ background: 'var(--color-base)', color: 'var(--color-text)' }}
    >
      {/* ── COURSE SWITCHER BAR ───────────────────────────────────────────── */}
      <div
        style={{
          borderBottom: '1px solid var(--color-border)',
          background: 'var(--color-surface)',
          padding: '8px 24px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          gap: '12px',
          flexWrap: 'wrap',
        }}
      >
        <span style={{ fontSize: '0.8125rem', color: 'var(--color-text-muted)', fontWeight: 500 }}>
          Available Courses:
        </span>
        <div style={{ display: 'inline-flex', padding: '3px', background: 'var(--color-base)', borderRadius: '8px', border: '1px solid var(--color-border)', gap: '4px' }}>
          <button
            onClick={() => handleNavigateToDbt(0)}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '6px',
              padding: '5px 12px',
              borderRadius: '6px',
              border: 'none',
              background: 'transparent',
              color: 'var(--color-text-secondary)',
              fontSize: '0.8125rem',
              fontWeight: 600,
              cursor: 'pointer',
              transition: 'all 0.15s ease',
            }}
            onMouseEnter={(e) => { e.currentTarget.style.color = 'var(--color-text)' }}
          >
            <span style={{ color: 'var(--color-accent-orange)' }}>⚡</span>
            <span>dbt Lab</span>
            <span style={{ fontSize: '0.6875rem', padding: '1px 6px', borderRadius: '10px', background: 'rgba(255,105,74,0.15)', color: 'var(--color-accent-orange)', fontWeight: 700 }}>
              59 Labs Live
            </span>
          </button>

          <button
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: '6px',
              padding: '5px 14px',
              borderRadius: '6px',
              border: '1px solid #E25A1C',
              background: 'rgba(226, 90, 28, 0.12)',
              color: '#E25A1C',
              fontSize: '0.8125rem',
              fontWeight: 700,
              cursor: 'default',
            }}
          >
            <SparkFlameIcon size={14} color="#E25A1C" />
            <span>Apache Spark Lab</span>
            <span style={{ fontSize: '0.6875rem', padding: '1px 6px', borderRadius: '10px', background: '#E25A1C', color: '#fff', fontWeight: 700 }}>
              45 Labs Live
            </span>
          </button>
        </div>
      </div>

      {/* ── HERO ──────────────────────────────────────────────────────────── */}
      <section style={{ maxWidth: '820px', margin: '0 auto', padding: '40px 32px 12px', textAlign: 'center' }}>
        <div style={{ display: 'inline-flex', alignItems: 'center', gap: '8px', padding: '4px 12px', borderRadius: '999px', background: 'rgba(226, 90, 28, 0.1)', border: '1px solid rgba(226, 90, 28, 0.3)', marginBottom: '16px' }}>
          <SparkFlameIcon size={14} color="#E25A1C" />
          <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.6875rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', color: '#E25A1C' }}>
            Interactive Big Data Track · PySpark & Spark SQL
          </span>
        </div>

        <div style={{ display: 'inline-flex', alignItems: 'baseline', gap: '10px', marginBottom: '12px', flexWrap: 'wrap', justifyContent: 'center' }}>
          <span style={{ fontFamily: 'var(--font-sans)', fontSize: '2.25rem', fontWeight: 800, color: '#E25A1C', letterSpacing: '-0.02em' }}>
            Apache Spark
          </span>
          <span style={{ fontFamily: 'var(--font-sans)', fontSize: '2.25rem', fontWeight: 800, color: 'var(--color-text)', letterSpacing: '-0.02em' }}>
            Lab
          </span>
        </div>

        <p style={{ fontFamily: 'var(--font-sans)', fontSize: '1.125rem', color: 'var(--color-text-secondary)', margin: '0 auto 8px', maxWidth: '620px', lineHeight: 1.45 }}>
          Master distributed in-memory computing with <strong style={{ color: '#E25A1C', fontWeight: 700 }}>Apache Spark</strong> & <strong style={{ color: 'var(--color-text)', fontWeight: 700 }}>PySpark</strong>.
        </p>
        <p style={{ fontFamily: 'var(--font-sans)', fontSize: '0.9375rem', color: 'var(--color-text-muted)', margin: '0 auto', maxWidth: '600px', lineHeight: 1.55 }}>
          From RDD fundamentals and DataFrame SQL transformations to analytical windowing, Broadcast Hash Joins, Delta Lake Medallion architectures, and real-time Structured Streaming.
        </p>

        {/* Path: Basics ▸ Intermediate ▸ Advanced */}
        <nav className="home-path" aria-label="Spark Course Stages" style={{ marginTop: '28px' }}>
          {SPARK_STAGES.map((stage) => {
            return (
              <span key={stage.id} className="home-pstep home-pstep--current">
                <span
                  className="home-pstep__name"
                  style={{
                    borderColor: '#E25A1C',
                    background: 'rgba(226, 90, 28, 0.08)',
                    color: 'var(--color-text)',
                  }}
                >
                  {stage.name} · {stage.labCount} Labs (Live)
                </span>
              </span>
            )
          })}
        </nav>
      </section>

      {/* ── STAGE CARDS ───────────────────────────────────────────────────── */}
      <section style={{ maxWidth: '840px', margin: '0 auto', padding: '24px 32px 8px' }}>
        <div className="home-cards">
          {SPARK_STAGES.map((stage) => {
            const isBasics = stage.id === 'basics'
            const isInter = stage.id === 'intermediate'
            const isLive = true
            const onStageClick = isBasics
              ? () => handleLaunchLab(resumeBasicsId)
              : isInter
              ? () => handleLaunchLab(resumeInterId)
              : () => handleLaunchLab(resumeAdvId)

            const stageHasProgress = isBasics ? hasBasicsProgress : isInter ? hasInterProgress : hasAdvProgress
            const completedCount = isBasics ? completedBasicsCount : isInter ? completedInterCount : completedAdvCount
            const resumeNum = isBasics ? resumeBasicsId - 100 : isInter ? resumeInterId - 112 : resumeAdvId - 127

            return (
              <div
                key={stage.id}
                className="home-card"
                style={{
                  borderColor: isLive ? 'var(--color-border)' : 'var(--color-border-subtle)',
                  background: 'var(--color-surface)',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '12px',
                  borderRadius: '12px',
                  padding: '22px',
                  position: 'relative',
                }}
              >
                <span
                  className="home-card__edge"
                  style={{ background: isLive ? '#E25A1C' : 'var(--color-border)' }}
                  aria-hidden="true"
                />

                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span
                    style={{
                      fontFamily: 'var(--font-mono)',
                      fontSize: '0.6875rem',
                      fontWeight: 700,
                      textTransform: 'uppercase',
                      letterSpacing: '0.06em',
                      padding: '3px 8px',
                      borderRadius: '999px',
                      background: isLive ? 'rgba(226, 90, 28, 0.15)' : 'rgba(128,128,128,0.1)',
                      color: isLive ? '#E25A1C' : 'var(--color-text-muted)',
                    }}
                  >
                    Phase {stage.id === 'basics' ? '1' : stage.id === 'intermediate' ? '2' : '3'} · {stage.labCount} Labs
                  </span>
                  <span
                    style={{
                      fontSize: '0.75rem',
                      color: isLive ? '#E25A1C' : 'var(--color-text-muted)',
                      fontFamily: 'var(--font-mono)',
                      fontWeight: 700,
                    }}
                  >
                    {isLive ? '● LIVE' : 'Upcoming'}
                  </span>
                </div>

                <div>
                  <h3 style={{ margin: 0, fontSize: '1.25rem', fontWeight: 700, color: 'var(--color-text)' }}>
                    {stage.name}
                  </h3>
                  <div style={{ fontSize: '0.8125rem', color: '#E25A1C', fontWeight: 600, marginTop: '2px' }}>
                    {stage.subtitle}
                  </div>
                </div>

                <p style={{ margin: 0, flexGrow: 1, fontSize: '0.875rem', lineHeight: 1.55, color: 'var(--color-text-secondary)' }}>
                  {stage.summary}
                </p>

                {/* Key highlights checklist */}
                <div style={{ background: 'var(--color-base)', padding: '10px 12px', borderRadius: '6px', border: '1px solid var(--color-border)' }}>
                  <div style={{ fontFamily: 'var(--font-mono)', fontSize: '0.6875rem', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--color-text-muted)', marginBottom: '6px', fontWeight: 600 }}>
                    Core Competencies:
                  </div>
                  <ul style={{ margin: 0, paddingLeft: '16px', fontSize: '0.8125rem', color: 'var(--color-text-secondary)', lineHeight: 1.45 }}>
                    {stage.highlights.slice(0, 3).map((h, i) => (
                      <li key={i} style={{ marginBottom: '3px' }}>{h}</li>
                    ))}
                  </ul>
                </div>

                {isLive && (
                  <div style={{ fontFamily: 'var(--font-mono)', fontSize: '0.6875rem', color: 'var(--color-text-muted)' }}>
                    {stageHasProgress ? (
                      <span style={{ color: 'var(--color-success)', fontWeight: 600 }}>
                        ✓ {completedCount} / {stage.labCount} Labs Completed
                      </span>
                    ) : (
                      `${stage.labCount} hands-on labs · Not started`
                    )}
                  </div>
                )}

                <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', marginTop: 'auto' }}>
                  {isLive ? (
                    <>
                      <button
                        onClick={onStageClick}
                        style={{
                          width: '100%',
                          display: 'inline-flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          gap: '8px',
                          padding: '10px 14px',
                          borderRadius: '8px',
                          border: 'none',
                          background: '#E25A1C',
                          color: '#fff',
                          fontFamily: 'var(--font-sans)',
                          fontSize: '0.875rem',
                          fontWeight: 700,
                          cursor: 'pointer',
                          transition: 'filter 0.15s ease',
                          boxShadow: '0 2px 8px rgba(226, 90, 28, 0.25)',
                        }}
                        onMouseEnter={(e) => { e.currentTarget.style.filter = 'brightness(1.1)' }}
                        onMouseLeave={(e) => { e.currentTarget.style.filter = 'none' }}
                      >
                        <span>
                          {isBasics
                            ? (stageHasProgress ? `Continue Basics (Lab ${resumeNum})` : 'Start Basics (Lab 1)')
                            : isInter
                            ? (stageHasProgress ? `Continue Intermediate (Lab ${resumeNum})` : 'Start Intermediate (Lab 13)')
                            : (stageHasProgress ? `Continue Advanced (Lab ${resumeNum})` : 'Start Advanced (Lab 28)')}
                        </span>
                        <span>→</span>
                      </button>

                      <button
                        onClick={() => setSelectedStage(stage.id)}
                        style={{
                          width: '100%',
                          display: 'inline-flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          gap: '6px',
                          padding: '7px 12px',
                          borderRadius: '6px',
                          border: '1px solid var(--color-border)',
                          background: 'transparent',
                          color: 'var(--color-text-secondary)',
                          fontFamily: 'var(--font-sans)',
                          fontSize: '0.75rem',
                          fontWeight: 600,
                          cursor: 'pointer',
                          transition: 'all 0.15s ease',
                        }}
                        onMouseEnter={(e) => { e.currentTarget.style.borderColor = '#E25A1C'; e.currentTarget.style.color = '#E25A1C' }}
                        onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--color-border)'; e.currentTarget.style.color = 'var(--color-text-secondary)' }}
                      >
                        <span>Description / Syllabus ({stage.labCount} Labs)</span>
                        <span>☰</span>
                      </button>
                    </>
                  ) : (
                    <button
                      onClick={() => setSelectedStage(stage.id)}
                      style={{
                        width: '100%',
                        display: 'inline-flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        gap: '6px',
                        padding: '9px 12px',
                        borderRadius: '6px',
                        border: '1px solid var(--color-border)',
                        background: 'transparent',
                        color: 'var(--color-text-muted)',
                        fontFamily: 'var(--font-sans)',
                        fontSize: '0.8125rem',
                        fontWeight: 600,
                        cursor: 'pointer',
                        transition: 'all 0.15s ease',
                      }}
                      onMouseEnter={(e) => { e.currentTarget.style.borderColor = 'var(--color-text-muted)' }}
                      onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--color-border)' }}
                    >
                      <span>Description / Syllabus ({stage.labCount} Labs)</span>
                      <span>→</span>
                    </button>
                  )}
                </div>
              </div>
            )
          })}
        </div>

        {hasAnySparkProgress && (
          <div style={{ marginTop: '16px', textAlign: 'center' }}>
            <button
              onClick={() => {
                if (window.confirm('Reset all Spark lab progress? This clears completed tasks for Spark labs 1 to 45.')) {
                  const state = useGameStore.getState()
                  const filtered = new Set(
                    [...state.completedTasks].filter((key) => {
                      const lessonNum = Number(key.split('.')[0])
                      return !(lessonNum >= 101 && lessonNum <= 199)
                    })
                  )
                  useGameStore.setState({ completedTasks: filtered })
                  try {
                    const raw = localStorage.getItem('transformation-lab-progress')
                    if (raw) {
                      const parsed = JSON.parse(raw)
                      parsed.completedTasks = [...filtered]
                      localStorage.setItem('transformation-lab-progress', JSON.stringify(parsed))
                    }
                  } catch {
                    /* ignore */
                  }
                  window.location.reload()
                }
              }}
              style={{
                background: 'transparent',
                color: 'var(--color-text-muted)',
                border: '1px solid var(--color-border)',
                borderRadius: '6px',
                padding: '7px 14px',
                fontFamily: 'var(--font-sans)',
                fontSize: '0.75rem',
                fontWeight: 500,
                cursor: 'pointer',
              }}
            >
              Restart Spark Progress
            </button>
          </div>
        )}

        {/* ── EARLY ACCESS NOTIFICATION BANNER ────────────────────────────── */}
        <div
          style={{
            marginTop: '28px',
            background: 'var(--color-surface)',
            border: '1px solid var(--color-border)',
            borderRadius: '12px',
            padding: '24px 28px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            gap: '24px',
            flexWrap: 'wrap',
          }}
        >
          <div style={{ flex: '1 1 320px' }}>
            <div style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', color: '#E25A1C', fontWeight: 700, fontSize: '0.8125rem', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: '4px', fontFamily: 'var(--font-mono)' }}>
              <span>🚀</span> 45 Hands-on Spark Labs are Live
            </div>
            <h4 style={{ margin: '0 0 6px', fontSize: '1.125rem', fontWeight: 700, color: 'var(--color-text)' }}>
              Basics, Intermediate & Advanced PySpark Labs Active
            </h4>
            <p style={{ margin: 0, fontSize: '0.875rem', color: 'var(--color-text-secondary)', lineHeight: 1.5 }}>
              Launch straight into RDDs, DataFrames, Windowing, Delta Lake Medallion pipelines, Catalyst query optimization, Dynamic Partition Pruning, and Structured Streaming above!
            </p>
          </div>

          <div style={{ flex: '0 1 340px', minWidth: '260px' }}>
            {subscribed ? (
              <div style={{ background: 'var(--color-success-bg)', border: '1px solid var(--color-success-border)', padding: '12px 16px', borderRadius: '8px', color: 'var(--color-success)', fontSize: '0.875rem', fontWeight: 600, display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span>✓</span>
                <span>You're on the list! We'll notify you on launch.</span>
              </div>
            ) : (
              <form onSubmit={handleSubscribe} style={{ display: 'flex', gap: '8px' }}>
                <input
                  type="email"
                  required
                  placeholder="Enter your email"
                  value={emailInput}
                  onChange={(e) => setEmailInput(e.target.value)}
                  style={{
                    flex: 1,
                    background: 'var(--color-base)',
                    border: '1px solid var(--color-border)',
                    borderRadius: '6px',
                    padding: '8px 12px',
                    fontSize: '0.875rem',
                    color: 'var(--color-text)',
                    fontFamily: 'var(--font-sans)',
                    outline: 'none',
                  }}
                />
                <button
                  type="submit"
                  style={{
                    background: '#E25A1C',
                    color: '#fff',
                    border: 'none',
                    borderRadius: '6px',
                    padding: '8px 16px',
                    fontFamily: 'var(--font-sans)',
                    fontSize: '0.875rem',
                    fontWeight: 600,
                    cursor: 'pointer',
                    whiteSpace: 'nowrap',
                  }}
                >
                  Notify Me
                </button>
              </form>
            )}
          </div>
        </div>
      </section>

      {/* ── COMPACT PRIMER ────────────────────────────────────────────────── */}
      <article
        style={{
          maxWidth: '740px',
          margin: '0 auto',
          padding: '32px 32px 16px',
          fontFamily: 'var(--font-sans)',
          fontSize: '0.9375rem',
          lineHeight: 1.65,
          color: 'var(--color-text-secondary)',
        }}
      >
        <SparkPrimerHeading>What is Apache Spark?</SparkPrimerHeading>
        <p style={{ margin: '0 0 18px' }}>
          <strong>Apache Spark</strong> is the open-source, multi-language distributed processing engine for large-scale data engineering, data science, and machine learning. Originally designed at UC Berkeley AMPLab to overcome the slow, disk-bound limitations of Hadoop MapReduce, Spark operates <strong>in-memory</strong>, processing petabytes of data across clusters up to 100× faster.
        </p>

        <SparkPrimerHeading>Why learn Apache Spark alongside dbt?</SparkPrimerHeading>
        <p style={{ margin: '0 0 18px' }}>
          Modern enterprise data stacks rely on a powerful two-tier partnership:
        </p>
        <ul style={{ margin: '0 0 18px', paddingLeft: '20px' }}>
          <li style={{ marginBottom: '8px' }}>
            <strong style={{ color: '#E25A1C' }}>Apache Spark & PySpark</strong> excel at high-scale data ingestion, heavy ETL, unstructured and semi-structured payloads (JSON, Avro, Parquet), raw-to-silver Lakehouse curation (Delta Lake, Apache Iceberg), and machine learning pipelines.
          </li>
          <li>
            <strong style={{ color: 'var(--color-accent-orange)' }}>dbt (Data Build Tool)</strong> coordinates warehouse modeling, semantic metrics, SQL dependency graphs, and enterprise data testing on curated tables.
          </li>
        </ul>
        <p style={{ margin: '0 0 18px' }}>
          Mastering both tools positions you at the absolute forefront of modern data engineering.
        </p>

        <SparkCallout title="Did you know? (Lazy DAG Evaluation)">
          When you write PySpark transformations like <code>df.filter(...).groupBy(...).select(...)</code>, Spark <strong>does not execute any calculations</strong> immediately! Instead, it compiles an optimized Directed Acyclic Graph (DAG) using the <strong>Catalyst Optimizer</strong>. Only when an <em>Action</em> (like <code>count()</code>, <code>collect()</code>, or <code>write()</code>) is invoked does Spark physically partition the data and execute across cluster workers.
        </SparkCallout>

        <SparkPrimerHeading>An Example Spark Execution DAG</SparkPrimerHeading>
        <p style={{ margin: '0 0 12px' }}>
          Unlike single-node SQL queries, Spark automatically splits work across cluster executors, separating stages whenever a data shuffle occurs:
        </p>
        <SparkDagDiagram />
        <p style={{ margin: '0 0 18px', fontSize: '0.875rem', color: 'var(--color-text-muted)' }}>
          Data is divided into <em>partitions</em>. Narrow transformations (Map, Filter) execute in parallel without network movement. Wide transformations (Joins, GroupBy) trigger a <em>Shuffle Exchange</em> across worker nodes.
        </p>

        <SparkPrimerHeading>Before you start</SparkPrimerHeading>
        <p style={{ margin: 0 }}>
          Just like our dbt labs, our upcoming Spark interactive quest will execute real code in-browser with zero local installation or cloud setup required. While we finalize the interactive runner, you can explore the complete 45-lab syllabus above or dive into our <strong>59 live dbt labs</strong>!
        </p>

        <div style={{ marginTop: '20px', textAlign: 'center' }}>
          <button
            onClick={() => handleNavigateToDbt(1)}
            style={{
              background: 'var(--color-accent-orange)',
              color: 'var(--color-on-accent)',
              border: 'none',
              borderRadius: '8px',
              padding: '10px 20px',
              fontSize: '0.9375rem',
              fontWeight: 700,
              cursor: 'pointer',
              display: 'inline-flex',
              alignItems: 'center',
              gap: '8px',
            }}
          >
            <span>Switch to dbt Course (Start Lesson 1)</span>
            <span>→</span>
          </button>
        </div>
      </article>

      {/* ── SYLLABUS MODAL ────────────────────────────────────────────────── */}
      {activeStage && (
        <div
          role="dialog"
          aria-modal="true"
          aria-labelledby="modal-title"
          style={{
            position: 'fixed',
            inset: 0,
            background: 'rgba(0,0,0,0.65)',
            zIndex: 1000,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            padding: '20px',
            backdropFilter: 'blur(3px)',
          }}
          onClick={() => setSelectedStage(null)}
        >
          <div
            style={{
              background: 'var(--color-surface)',
              border: '1px solid var(--color-border)',
              borderRadius: '12px',
              width: 'min(700px, 100%)',
              maxHeight: '85vh',
              display: 'flex',
              flexDirection: 'column',
              boxShadow: '0 20px 40px rgba(0,0,0,0.4)',
              overflow: 'hidden',
            }}
            onClick={(e) => e.stopPropagation()}
          >
            {/* Modal Header */}
            <div
              style={{
                padding: '20px 24px',
                borderBottom: '1px solid var(--color-border)',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
              }}
            >
              <div>
                <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.6875rem', fontWeight: 700, color: '#E25A1C', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                  Apache Spark Lab · Syllabus
                </span>
                <h3 id="modal-title" style={{ margin: '4px 0 0', fontSize: '1.25rem', fontWeight: 700, color: 'var(--color-text)' }}>
                  {activeStage.name}: {activeStage.subtitle} ({activeStage.labCount} Labs)
                </h3>
              </div>
              <button
                onClick={() => setSelectedStage(null)}
                style={{
                  background: 'transparent',
                  border: 'none',
                  color: 'var(--color-text-muted)',
                  fontSize: '1.5rem',
                  cursor: 'pointer',
                  padding: '4px 8px',
                }}
              >
                ✕
              </button>
            </div>

            {/* Modal Content - List of Labs */}
            <div style={{ padding: '20px 24px', overflowY: 'auto', flex: 1, display: 'flex', flexDirection: 'column', gap: '10px' }}>
              {activeStage.topics.map((topic, idx) => {
                const labId =
                  activeStage.id === 'basics'
                    ? 101 + idx
                    : activeStage.id === 'intermediate'
                    ? 113 + idx
                    : 128 + idx
                const isDone = labId ? lessonCompleted(completedTasks, labId) : false
                const isPlayable = labId !== null

                return (
                  <div
                    key={idx}
                    style={{
                      padding: '12px 14px',
                      borderRadius: '8px',
                      background: 'var(--color-base)',
                      border: isDone ? '1px solid var(--color-success-border)' : '1px solid var(--color-border)',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      gap: '14px',
                    }}
                  >
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '3px', flexWrap: 'wrap' }}>
                        <span style={{ fontWeight: 700, fontSize: '0.875rem', color: 'var(--color-text)' }}>
                          {topic.title}
                        </span>
                        {isDone && (
                          <span
                            style={{
                              fontSize: '0.6875rem',
                              padding: '1px 6px',
                              borderRadius: '4px',
                              background: 'var(--color-success-bg)',
                              color: 'var(--color-success)',
                              fontWeight: 700,
                            }}
                          >
                            ✓ Completed
                          </span>
                        )}
                      </div>
                      <div style={{ fontSize: '0.8125rem', color: 'var(--color-text-secondary)', lineHeight: 1.45 }}>
                        {topic.desc}
                      </div>
                    </div>
                    {isPlayable && (
                      <button
                        onClick={() => {
                          setSelectedStage(null)
                          handleLaunchLab(labId)
                        }}
                        style={{
                          flexShrink: 0,
                          background: isDone ? 'var(--color-base)' : '#E25A1C',
                          color: isDone ? 'var(--color-text)' : '#fff',
                          border: isDone ? '1px solid var(--color-border)' : 'none',
                          borderRadius: '6px',
                          padding: '6px 12px',
                          fontSize: '0.75rem',
                          fontWeight: 700,
                          cursor: 'pointer',
                          whiteSpace: 'nowrap',
                          transition: 'filter 0.15s ease',
                        }}
                        onMouseEnter={(e) => { e.currentTarget.style.filter = 'brightness(1.1)' }}
                        onMouseLeave={(e) => { e.currentTarget.style.filter = 'none' }}
                      >
                        {isDone ? 'Review Lab' : 'Launch Lab →'}
                      </button>
                    )}
                  </div>
                )
              })}
            </div>

            {/* Modal Footer */}
            <div
              style={{
                padding: '14px 24px',
                borderTop: '1px solid var(--color-border)',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                background: 'var(--color-surface)',
              }}
            >
              <span style={{ fontSize: '0.8125rem', color: 'var(--color-text-muted)' }}>
                {activeStage.labCount} hands-on labs in this stage
              </span>
              <button
                onClick={() => setSelectedStage(null)}
                style={{
                  background: 'var(--color-base)',
                  border: '1px solid var(--color-border)',
                  color: 'var(--color-text)',
                  padding: '6px 14px',
                  borderRadius: '6px',
                  fontSize: '0.8125rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                }}
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ── FOOTER ────────────────────────────────────────────────────────── */}
      <footer
        style={{
          maxWidth: '740px',
          margin: '32px auto 40px',
          padding: '24px 32px 0',
          borderTop: '1px solid var(--color-border)',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          fontSize: '0.8125rem',
          color: 'var(--color-text-muted)',
          flexWrap: 'wrap',
          gap: '12px',
        }}
      >
        <div>
          <span>Data Transformation Lab · Apache Spark Course</span>
        </div>
        <div style={{ display: 'flex', gap: '16px' }}>
          <button
            onClick={() => handleNavigateToDbt(0)}
            style={{ background: 'transparent', border: 'none', color: 'var(--color-accent-orange)', cursor: 'pointer', fontSize: '0.8125rem', padding: 0 }}
          >
            ← Back to dbt Course
          </button>
          <a
            href="https://datagym.io"
            target="_blank"
            rel="noopener noreferrer"
            style={{ color: 'var(--color-text-muted)', textDecoration: 'none' }}
          >
            DataGym.io
          </a>
        </div>
      </footer>
    </div>
  )
}

function SparkPrimerHeading({ children }: { children: React.ReactNode }) {
  return (
    <h2 style={{ fontFamily: 'var(--font-sans)', fontSize: '1rem', fontWeight: 700, color: 'var(--color-text)', margin: '0 0 8px' }}>
      {children}
    </h2>
  )
}

function SparkCallout({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div
      style={{
        borderLeft: '3px solid #E25A1C',
        background: 'var(--color-surface)',
        borderRadius: '0 8px 8px 0',
        padding: '12px 16px',
        margin: '0 0 18px',
      }}
    >
      <div style={{ fontWeight: 700, color: 'var(--color-text)', marginBottom: '4px' }}>{title}</div>
      <div>{children}</div>
    </div>
  )
}

/**
 * Visual SVG Diagram of the Apache Spark execution pipeline:
 * Driver (SparkSession) -> Stage 0 (Narrow: Partitions) -> Shuffle Exchange -> Stage 1 (Wide: Aggregation) -> Sink
 */
function SparkDagDiagram() {
  const nodes = [
    { id: 'driver', label: 'Spark Driver\n(SparkSession)', layer: 'driver', x: 0, y: 0.5 },
    { id: 'stage0', label: 'Stage 0 (Narrow)\nMap & Filter (200 Partitions)', layer: 'narrow', x: 1, y: 0.5 },
    { id: 'shuffle', label: 'Shuffle Exchange\nNetwork Partition Transfer', layer: 'shuffle', x: 2, y: 0.5 },
    { id: 'stage1', label: 'Stage 1 (Wide)\nJoin & Hash Aggregate', layer: 'wide', x: 3, y: 0.5 },
    { id: 'output', label: 'Action Output\nDelta Lake / Parquet', layer: 'sink', x: 4, y: 0.5 },
  ]

  const edges = [
    ['driver', 'stage0'],
    ['stage0', 'shuffle'],
    ['shuffle', 'stage1'],
    ['stage1', 'output'],
  ]

  const colWidth = 145, rowHeight = 70, nodeW = 125, nodeH = 44, padX = 14, padY = 18
  const W = colWidth * 5 + padX * 2 - (colWidth - nodeW)
  const H = rowHeight + padY * 2

  const pos = (n: (typeof nodes)[number]) => ({
    cx: padX + n.x * colWidth + nodeW / 2,
    cy: padY + n.y * rowHeight + nodeH / 2,
  })

  return (
    <div style={{ border: '1px solid var(--color-border)', borderRadius: '8px', background: 'var(--color-surface)', padding: '16px', overflowX: 'auto', margin: '0 0 12px' }}>
      <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} style={{ display: 'block', margin: '0 auto', maxWidth: '100%' }}>
        <defs>
          <marker id="spark-arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
            <path d="M0,0 L0,8 L8,4 z" fill="#E25A1C" />
          </marker>
        </defs>

        {edges.map(([from, to]) => {
          const f = nodes.find((n) => n.id === from)!
          const tn = nodes.find((n) => n.id === to)!
          const a = pos(f)
          const b = pos(tn)
          return (
            <line
              key={`${from}-${to}`}
              x1={a.cx + nodeW / 2}
              y1={a.cy}
              x2={b.cx - nodeW / 2}
              y2={b.cy}
              stroke="var(--color-border)"
              strokeWidth="2"
              markerEnd="url(#spark-arrow)"
            />
          )
        })}

        {nodes.map((n) => {
          const p = pos(n)
          const isHighlight = n.layer === 'driver' || n.layer === 'shuffle'
          return (
            <g key={n.id} transform={`translate(${p.cx - nodeW / 2}, ${p.cy - nodeH / 2})`}>
              <rect
                width={nodeW}
                height={nodeH}
                rx="6"
                fill={isHighlight ? 'rgba(226, 90, 28, 0.12)' : 'var(--color-base)'}
                stroke={isHighlight ? '#E25A1C' : 'var(--color-border)'}
                strokeWidth="1.5"
              />
              <text
                x={nodeW / 2}
                y={nodeH / 2 - 4}
                textAnchor="middle"
                fontSize="10"
                fontFamily="JetBrains Mono, monospace"
                fontWeight="700"
                fill={isHighlight ? '#E25A1C' : 'var(--color-text)'}
              >
                {n.label.split('\n')[0]}
              </text>
              <text
                x={nodeW / 2}
                y={nodeH / 2 + 10}
                textAnchor="middle"
                fontSize="9"
                fontFamily="var(--font-sans)"
                fill="var(--color-text-muted)"
              >
                {n.label.split('\n')[1] || ''}
              </text>
            </g>
          )
        })}
      </svg>
    </div>
  )
}

export function SparkFlameIcon({ size = 16, color = 'currentColor' }: { size?: number; color?: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" style={{ display: 'inline-block', verticalAlign: 'middle' }}>
      <path
        d="M12 2C11.5 4.5 9 6.5 8 9C7 11.5 7.5 13.5 8.5 15C6.5 14 5 12 5 9.5C3 13 3 17 6.5 20C10 23 15 22.5 18 19.5C21 16.5 21 12 18.5 8C18 10 16.5 11.5 15 11.5C15 8.5 14 4.5 12 2Z"
        fill={color}
      />
      <path
        d="M12 14C11.5 15 11 16 11.5 17C12 18 13 18.5 14 18C15 17.5 15.5 16 15 15C14.5 14 13.5 13 12 14Z"
        fill="#FFE28A"
      />
    </svg>
  )
}
