/*
  model:   mart/fact_events_daily.sql
  grain:   one row per user per calendar day (UTC)
  owner:   analytics-eng
  refresh: every 6h via scheduled dbt run --select fact_events_daily

  incremental logic:
    on full refresh  → rebuilds entire history from 2023-01-01
    on incremental   → merges the last 3 days to handle late-arriving events
*/

{{
  config(
    materialized       = 'incremental',
    unique_key         = ['activity_date', 'user_id'],
    incremental_strategy = 'merge',
    cluster_by         = ['activity_date', 'plan_tier'],
    on_schema_change   = 'fail'
  )
}}


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- CTE 1: normalize event timestamps to UTC date buckets
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WITH events_with_date AS (

    SELECT
        event_id,
        user_id,
        event_type,
        event_ts,
        CONVERT_TIMEZONE('UTC', event_ts)::DATE   AS activity_date,
        session_id

    FROM {{ source('raw', 'events') }}
    WHERE
        -- exclude backfill noise and future-dated rows
        CONVERT_TIMEZONE('UTC', event_ts)::DATE
            BETWEEN '2023-01-01' AND CURRENT_DATE()
        AND user_id IS NOT NULL

        -- on incremental runs, only process the last 3 days
        -- (3-day lookback catches late-arriving Segment events)
        {% if is_incremental() %}
        AND CONVERT_TIMEZONE('UTC', event_ts)::DATE
            >= DATEADD('day', -3, CURRENT_DATE())
        {% endif %}

),


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- CTE 2: tag activation events
--   activation = first meaningful feature engagement
--   source of truth for this list: tracking plan in Notion
--   to add a new activation event, add it here only.
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
activation_events AS (

    SELECT
        event_id,
        user_id,
        activity_date,
        CASE
            WHEN event_type IN (
                'export_run',
                'integration_connected',
                'invite_sent',
                'template_published'
            ) THEN 1
            ELSE 0
        END  AS is_activation_event

    FROM events_with_date

),


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- CTE 3: aggregate to user × day grain
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
daily_user_activity AS (

    SELECT
        user_id,
        activity_date,
        COUNT(*)                          AS total_events,
        SUM(is_activation_event)          AS activation_events,
        COUNT(DISTINCT session_id)        AS unique_sessions

    FROM activation_events
    GROUP BY 1, 2

),


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- CTE 4: enrich with user and account dimensions
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
enriched AS (

    SELECT
        d.activity_date,
        d.user_id,
        u.account_id,
        u.plan_tier,
        u.signup_channel,
        a.company_size_band,
        a.mrr,

        d.total_events,
        d.activation_events,
        d.unique_sessions,

        -- tenure in days at the time of activity
        DATEDIFF('day', u.created_at::DATE, d.activity_date)
                                           AS days_since_signup,

        -- lifetime activation flag: 1+ activation events ever
        -- cumulative sum resets to TRUE and stays TRUE permanently
        CASE
            WHEN SUM(d.activation_events)
                     OVER (PARTITION BY d.user_id
                           ORDER BY     d.activity_date
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                 > 0 THEN TRUE
            ELSE FALSE
        END                                AS is_activated

    FROM      daily_user_activity  d
    JOIN      {{ source('raw', 'users') }}    u  ON d.user_id    = u.user_id
    LEFT JOIN {{ source('raw', 'accounts') }} a  ON u.account_id = a.account_id

),


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- CTE 5: rolling window metrics + churn risk signal
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
windowed AS (

    SELECT
        *,

        -- 7-day rolling event volume (L7 stickiness proxy)
        -- ROWS frame is explicit to avoid Snowflake default RANGE behavior
        SUM(total_events)
            OVER (
                PARTITION BY user_id
                ORDER BY     activity_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            )                              AS rolling_7d_events,

        -- days since last active (gap detection)
        -- NULL on first-ever activity row; handle in consumers
        DATEDIFF(
            'day',
            LAG(activity_date) OVER (
                PARTITION BY user_id
                ORDER BY     activity_date
            ),
            activity_date
        )                                  AS days_since_last_active,

        -- churn risk: activated but silent for 14+ days
        -- threshold is a business decision — do not hardcode in dashboards
        CASE
            WHEN is_activated = TRUE
             AND DATEDIFF(
                     'day',
                     LAG(activity_date) OVER (
                         PARTITION BY user_id
                         ORDER BY activity_date
                     ),
                     activity_date
                 ) >= 14
            THEN TRUE
            ELSE FALSE
        END                                AS is_churn_risk

    FROM enriched

)


-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Final select: expose clean, typed output columns
-- column order: keys → dimensions → measures → flags
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SELECT
    activity_date,
    user_id,
    account_id,
    plan_tier,
    signup_channel,
    company_size_band,
    mrr,
    total_events,
    activation_events,
    unique_sessions,
    days_since_signup,
    is_activated,
    rolling_7d_events,
    days_since_last_active,
    is_churn_risk

FROM windowed
ORDER BY
    activity_date DESC,
    user_id       ASC
