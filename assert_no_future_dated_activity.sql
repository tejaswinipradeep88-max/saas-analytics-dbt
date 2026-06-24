/*
  test: assert_no_future_dated_activity.sql
  purpose: catch ETL bugs that land events with future timestamps
  fails if: any activity_date is after today's UTC date
*/

SELECT
    activity_date,
    COUNT(*) AS row_count

FROM {{ ref('fact_events_daily') }}

WHERE activity_date > CURRENT_DATE()

GROUP BY 1

HAVING COUNT(*) > 0
