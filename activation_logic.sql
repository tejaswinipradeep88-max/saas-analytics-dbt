/*
  macro: activation_event_types
  purpose: single source of truth for which event_types count as activation
  usage:   WHERE event_type IN ({{ activation_event_types() }})

  to add a new activation event:
    1. update the list below
    2. run: dbt run --full-refresh --select fact_events_daily
    3. update the tracking plan doc in Notion
*/

{% macro activation_event_types() %}
    (
        'export_run',
        'integration_connected',
        'invite_sent',
        'template_published'
    )
{% endmacro %}


/*
  macro: churn_risk_threshold_days
  purpose: configurable silence threshold for is_churn_risk flag
  default: 14 days
  override in dbt_project.yml vars if threshold changes
*/

{% macro churn_risk_threshold_days() %}
    {{ var('churn_risk_days', 14) }}
{% endmacro %}
