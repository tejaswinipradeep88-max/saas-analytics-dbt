/*
  test: assert_churn_risk_requires_activation.sql
  purpose: churn risk is only meaningful for users who activated
           a non-activated user flagged as churn_risk is a logic error
  fails if: any row has is_churn_risk = TRUE and is_activated = FALSE
*/

SELECT
    user_id,
    activity_date,
    is_activated,
    is_churn_risk

FROM {{ ref('fact_events_daily') }}

WHERE
    is_churn_risk = TRUE
    AND is_activated = FALSE
