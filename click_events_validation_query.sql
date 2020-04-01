/*
  This query is to validate the results of the page events clicks analysis
  
  It aggregates clicks, sessions, pageviews, and CTR by Site, Platform, and Event category Supergroup
  
  To compare with data analysis
*/

WITH events AS
(
  SELECT
    Site
    ,Platform
    ,CASE
      WHEN REGEXP_CONTAINS(EventCategory, '[Ee]ngagement') THEN 'Bottom Page Recommendation'
      WHEN REGEXP_CONTAINS(EventCategory, '[Ll]ink') THEN 'Article Links'
      WHEN REGEXP_CONTAINS(EventCategory, '[Rr]ail') AND REGEXP_CONTAINS(EventAction, '[Ww]idget [Rr]elated [Ss]tories') THEN 'Read Next Right Rail'
      WHEN REGEXP_CONTAINS(EventCategory, '[Rr]ail') THEN 'Other Right Rail'
      ELSE EventCategory
    END AS Supergroup
    ,SUM(EventCount) As TotalClicks
  FROM
    `dataeng-214618.PROD_Audience.events_*`
  WHERE
    DatePT = '2020-01-13'
    AND _TABLE_SUFFIX IN ('grt', 'hl', 'mnt')
    AND
    (
      msiteId = 'None'
      OR msiteId = ''
      OR msiteID IS NULL
    )
    AND
    (
      (
        EventCategory = 'Right Rail'
        --AND EventAction = 'Widget Related Stories'
      )
      OR (
        REGEXP_CONTAINS(EventCategory, '(Desktop|Mobile|Tablet) engagement')
        AND REGEXP_CONTAINS(EventAction, 'click$')
      )
      OR REGEXP_CONTAINS(EventCategory, 'Article Body - .+')
    )
  GROUP BY
    1, 2, 3
), traffic AS
(
  SELECT
    Site
    ,Platform
    ,SUM(SessionCount) AS TotalSessions
    ,SUM(PageViews) AS TotalPageViews
    ,SAFE_DIVIDE(SUM(SecondsOnSite), SUM(SessionCount)) AS AvgSessionDuration
  FROM
    `dataeng-214618.PROD_Audience.traffic_*`
  WHERE
    DatePT = '2020-01-13'
    AND _TABLE_SUFFIX IN ('grt', 'hl', 'mnt')
    AND
    (
      msiteId = 'None'
      OR msiteId = ''
      OR msiteID IS NULL
    )
  GROUP BY
    1, 2
  HAVING
    TotalPageviews > 0
)

SELECT
  traffic.Site
  ,traffic.Platform
  ,events.Supergroup
  ,events.TotalClicks
  ,traffic.TotalSessions
  ,traffic.TotalPageViews
  ,traffic.AvgSessionDuration
  ,SAFE_DIVIDE(TotalClicks, TotalPageViews) AS ClicksPerPageView
FROM
  events
RIGHT JOIN
  traffic
  ON 
  events.Site = traffic.Site
  AND events.Platform = traffic.Platform
ORDER BY
  1, 3 DESC