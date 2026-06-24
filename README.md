# saas_analytics · dbt project

Analytics engineering proof-of-work project demonstrating production-grade dbt
patterns for a B2B SaaS PLG company on Snowflake.

---

## Project structure

```
saas_analytics/
├── dbt_project.yml                     # project config + materialization defaults
├── macros/
│   └── activation_logic.sql            # activation event list + churn threshold
├── models/
│   ├── staging/
│   │   └── sources.yml                 # raw source declarations + freshness SLAs
│   └── mart/
│       ├── fact_events_daily.sql       # core daily activity fact table
│       └── schema.yml                  # column docs + dbt tests
└── tests/
    ├── assert_no_future_dated_activity.sql
    └── assert_churn_risk_requires_activation.sql
```

---

## Core model: `fact_events_daily`

**Grain:** one row per `user_id` × `activity_date` (UTC)
**Materialization:** incremental, merge strategy, clustered on `(activity_date, plan_tier)`
**Refresh cadence:** every 6 hours

### Key metrics produced

| Column | Description |
|---|---|
| `total_events` | All events for the user on this date |
| `activation_events` | Subset matching the activation event list |
| `is_activated` | Cumulative: TRUE once user hits first activation event |
| `rolling_7d_events` | Trailing 7-day event volume (L7 stickiness proxy) |
| `days_since_last_active` | Gap since prior active day (NULL on first row) |
| `is_churn_risk` | Activated user silent for 14+ days |

### Activation event set

Defined once in `macros/activation_logic.sql`. Current events:

- `export_run`
- `integration_connected`
- `invite_sent`
- `template_published`

To update: edit the macro → run `dbt run --full-refresh --select fact_events_daily` → update the Notion tracking plan.

### Churn risk threshold

Defaults to 14 days. Override in `dbt_project.yml`:

```yaml
vars:
  churn_risk_days: 21
```

---

## Running the project

```bash
# install dependencies
dbt deps

# validate sources are fresh
dbt source freshness

# full refresh (initial build or after activation list change)
dbt run --full-refresh --select fact_events_daily

# incremental run (normal cadence)
dbt run --select fact_events_daily

# run all tests
dbt test --select fact_events_daily

# generate and serve docs
dbt docs generate
dbt docs serve
```

---

## Design decisions

**Why incremental with a 3-day lookback?**
Segment delivers events with up to 48h latency for mobile clients. A 3-day merge window catches late arrivals without reprocessing the full history on every run.

**Why cluster on `(activity_date, plan_tier)`?**
Downstream queries almost always filter on a date range and segment by plan tier. Snowflake micro-partition pruning on this cluster reduces query costs by 60–80% versus an unclustered table at scale.

**Why are activation events in a macro, not a seed?**
Seeds require a dbt run to update. A macro update takes effect on the next model run with no additional step, making it faster to iterate with the product team when the activation definition evolves.

---

## Downstream consumers

- **Looker:** growth dashboard (DAU, activation rate, L7 stickiness)
- **Amplitude:** cohort exports keyed on `is_activated` + `days_since_signup`
- **Slack alerts:** CSM churn-risk digest (filters `is_churn_risk = TRUE` + `plan_tier != 'free'`)
- **Exec digest:** weekly rollup aggregated from this model
