import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useGameStore } from '../store/gameStore'
import { renderInline } from './Markdownish'

/**
 * Celebration block shown in LessonPanel after the final lesson's tasks all
 * complete. Renders a small confetti burst (pure CSS, no library), a headline,
 * suggested next steps, and a share link. Replaces the previous one-line
 * "you've finished" success box.
 */
export default function CourseComplete() {
  const { t } = useTranslation()
  const [confetti] = useState(() => {
    const palette = ['var(--color-accent-orange)', 'var(--color-success)', 'var(--color-warning)', 'var(--color-text)']
    return Array.from({ length: 22 }, (_, i) => ({
      left: Math.random() * 100,
      delay: Math.random() * 600,
      color: palette[i % palette.length],
      size: 4 + Math.random() * 4,
    }))
  })

  const loadLesson = useGameStore((s) => s.loadLesson)
  const currentLessonId = useGameStore((s) => s.currentLessonId)
  const isSpark = currentLessonId >= 101
  const isAdvanced = !isSpark && currentLessonId >= 35
  const isIntermediate = !isSpark && currentLessonId >= 15 && currentLessonId < 35
  const linkedInUrl = `https://www.linkedin.com/sharing/share-offsite/?url=${encodeURIComponent('https://transformation-lab.datagym.io')}`

  return (
    <div
      style={{
        position: 'relative',
        overflow: 'hidden',
        border: '1px solid var(--color-success-border)',
        background: 'var(--color-success-bg)',
        borderRadius: '8px',
        padding: '20px 18px 18px',
      }}
    >
      <div aria-hidden="true" style={{ position: 'absolute', inset: 0, pointerEvents: 'none' }}>
        {confetti.map((c, i) => (
          <span
            key={i}
            className="sparkle"
            style={{
              left: `${c.left}%`,
              top: '50%',
              width: `${c.size}px`,
              height: `${c.size}px`,
              background: c.color,
              animationDelay: `${c.delay}ms`,
            }}
          />
        ))}
      </div>

      <div
        style={{
          color: isSpark ? '#E25A1C' : 'var(--color-accent-orange)',
          fontFamily: 'JetBrains Mono, monospace',
          fontSize: '0.6875rem',
          textTransform: 'uppercase',
          letterSpacing: '0.12em',
          fontWeight: 600,
          marginBottom: '8px',
        }}
      >
        {isSpark
          ? 'APACHE SPARK COURSE COMPLETE (45 LABS MASTERED)'
          : isAdvanced
          ? 'ADVANCED COURSE COMPLETE'
          : isIntermediate
          ? 'INTERMEDIATE LEVEL COMPLETE'
          : t('courseComplete.eyebrow')}
      </div>
      <h3
        style={{
          margin: '0 0 10px',
          color: 'var(--color-text)',
          fontFamily: 'var(--font-sans)',
          fontSize: '1.0625rem',
          fontWeight: 700,
          lineHeight: 1.3,
        }}
      >
        {isSpark
          ? "You've mastered Apache Spark, Lakehouses & Structured Streaming!"
          : isAdvanced
          ? "You've mastered advanced analytics engineering & enterprise dbt!"
          : isIntermediate
          ? "You've mastered intermediate dbt engineering!"
          : t('courseComplete.title')}
      </h3>
      <p
        style={{
          margin: '0 0 14px',
          color: 'var(--color-text-secondary)',
          fontSize: '0.875rem',
          lineHeight: 1.6,
        }}
      >
        {isSpark
          ? "You've built and orchestrated production-grade big data systems: RDD lineage, DataFrame transformations, analytical windowing, Broadcast Hash Joins, Delta Lake Medallion pipelines, Catalyst query plans, Adaptive Query Execution (AQE), Dynamic Partition Pruning, and real-time Structured Streaming with event-time watermarking."
          : isAdvanced
          ? "You've built and orchestrated production-grade data platforms: high-performance merge strategies, custom generic test macros, unit testing, schema enforcement contracts, Semantic Layer metrics, exposures, and zero-downtime blue-green deployments."
          : isIntermediate
          ? "You've tackled the real patterns production teams lean on: incremental processing, slowly changing dimensions (SCD2), modular Jinja macros, multi-schema architecture, and model contracts."
          : renderInline(t('courseComplete.body'))}
      </p>

      <div style={{ marginBottom: '12px' }}>
        <SubLabel>{t('courseComplete.whatNext')}</SubLabel>
        {isSpark ? (
          <ul style={{ margin: '6px 0 0', padding: 0, listStyle: 'none', display: 'flex', flexDirection: 'column', gap: '6px' }}>
            <NextStep>
              <span>Deploy to cloud clusters on AWS EMR, GCP Dataproc, or Databricks: </span>
              <ExtLink href="https://spark.apache.org/docs/latest/cloud-integration.html">Spark Cloud Integration Guide</ExtLink>
            </NextStep>
            <NextStep>
              <span>Build open lakehouse architectures with Delta Lake: </span>
              <ExtLink href="https://docs.delta.io/">Delta Lake Documentation</ExtLink>
            </NextStep>
            <NextStep>
              <span>Explore data warehouse modeling in our 59-lab dbt course: </span>
              <button
                onClick={() => {
                  void loadLesson(0)
                  window.history.pushState(null, '', '/')
                  window.dispatchEvent(new PopStateEvent('popstate'))
                }}
                style={{
                  background: 'none',
                  border: 'none',
                  color: 'var(--color-accent-orange)',
                  fontWeight: 600,
                  cursor: 'pointer',
                  padding: 0,
                  textDecoration: 'underline',
                  fontSize: '0.8125rem',
                }}
              >
                Switch to dbt Lab Course (Start Lesson 1)
              </button>
            </NextStep>
          </ul>
        ) : isAdvanced ? (
          <ul style={{ margin: '6px 0 0', padding: 0, listStyle: 'none', display: 'flex', flexDirection: 'column', gap: '6px' }}>
            <NextStep>
              <span>Deploy to enterprise production with automated CI: </span>
              <ExtLink href="https://docs.getdbt.com/docs/deploy/deployments">dbt Cloud & orchestrator deployments</ExtLink>
            </NextStep>
            <NextStep>
              <span>Explore dbt Mesh cross-project dependencies: </span>
              <ExtLink href="https://docs.getdbt.com/docs/collaborate/govern/about-mesh">dbt Mesh architecture guide</ExtLink>
            </NextStep>
            <NextStep>
              <span>Contribute to open source dbt packages & adapters: </span>
              <ExtLink href="https://github.com/dbt-labs/dbt-core">dbt-core GitHub repository</ExtLink>
            </NextStep>
          </ul>
        ) : isIntermediate ? (
          <ul style={{ margin: '6px 0 0', padding: 0, listStyle: 'none', display: 'flex', flexDirection: 'column', gap: '6px' }}>
            <NextStep>
              <span>Deploy your dbt models to Snowflake, BigQuery, or Databricks: </span>
              <ExtLink href="https://docs.getdbt.com/docs/core/connect-data-platform/about-core-connections">Adapter setup guide</ExtLink>
            </NextStep>
            <NextStep>
              <span>Automate runs with orchestration: </span>
              <ExtLink href="https://docs.getdbt.com/docs/deploy/deployments">dbt deployments & schedules</ExtLink>
            </NextStep>
            <NextStep>
              <span>Explore Advanced dbt: </span>
              <ExtLink href="https://docs.getdbt.com/docs/build/python-models">Python models</ExtLink>
              <span> · </span>
              <ExtLink href="https://docs.getdbt.com/docs/build/semantic-models">Semantic Layer & Metrics</ExtLink>
            </NextStep>
          </ul>
        ) : (
          <ul style={{ margin: '6px 0 0', padding: 0, listStyle: 'none', display: 'flex', flexDirection: 'column', gap: '6px' }}>
            <NextStep>
              {t('courseComplete.step1Lead')}
              <ExtLink href="https://docs.getdbt.com/docs/core/installation-overview">{t('courseComplete.step1Link')}</ExtLink>
            </NextStep>
            <NextStep>
              {t('courseComplete.step2Lead')}
              <ExtLink href="https://github.com/dbt-labs/jaffle-shop">{t('courseComplete.step2Link')}</ExtLink>
              {t('courseComplete.step2Tail')}
            </NextStep>
            <NextStep>
              {t('courseComplete.step3Lead')}
              <ExtLink href="https://docs.getdbt.com/docs/build/jinja-macros">{t('courseComplete.step3Macros')}</ExtLink>
              {t('courseComplete.step3Sep')}
              <ExtLink href="https://docs.getdbt.com/docs/build/incremental-models">{t('courseComplete.step3Incremental')}</ExtLink>
              {t('courseComplete.step3Sep')}
              <ExtLink href="https://docs.getdbt.com/docs/build/snapshots">{t('courseComplete.step3Snapshots')}</ExtLink>
              {t('courseComplete.step3Sep')}
              <ExtLink href="https://docs.getdbt.com/docs/build/packages">{t('courseComplete.step3Packages')}</ExtLink>
              {t('courseComplete.step3End')}
            </NextStep>
          </ul>
        )}
      </div>

      <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
        <a
          href={linkedInUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="btn-primary"
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: '6px',
            padding: '8px 14px',
            fontSize: '0.8125rem',
            textDecoration: 'none',
          }}
        >
          {t('courseComplete.shareLinkedIn')}
        </a>
        <button
          onClick={() => void loadLesson(0)}
          style={{
            padding: '8px 14px',
            fontSize: '0.8125rem',
            color: 'var(--color-text-muted)',
            background: 'transparent',
            border: '1px solid var(--color-border)',
            borderRadius: '6px',
            cursor: 'pointer',
            fontFamily: 'var(--font-sans)',
            fontWeight: 500,
          }}
        >
          {t('courseComplete.backToIntro')}
        </button>
      </div>
    </div>
  )
}

function SubLabel({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        color: 'var(--color-text-muted)',
        fontFamily: 'JetBrains Mono, monospace',
        fontSize: '0.625rem',
        textTransform: 'uppercase',
        letterSpacing: '0.1em',
      }}
    >
      {children}
    </div>
  )
}

function NextStep({ children }: { children: React.ReactNode }) {
  return (
    <li style={{ display: 'flex', gap: '8px', color: 'var(--color-text-secondary)', fontSize: '0.8125rem', lineHeight: 1.55 }}>
      <span style={{ color: 'var(--color-accent-orange)', flexShrink: 0 }}>→</span>
      <span>{children}</span>
    </li>
  )
}

function ExtLink({ href, children }: { href: string; children: React.ReactNode }) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      style={{ color: 'var(--color-accent-orange)', textDecoration: 'underline' }}
    >
      {children}
    </a>
  )
}
