/*
  This query is to pull raw data to analyse Read Next, right rail, and links clicks
  
  It joins total clicks metrics from the events tables grouped by Date, Platform, PagePath, Site, Event Category, action and label
  with metrics from the traffic tables grouped by Date, Platform, PagePath, and Site.
  
  Originally I included K1, K2, and mSite ID as fields as well, but this was resulting in duplicated rows because of differnt values in the joined tables
  
  Events are joined to traffic via a right join since many site/pagepath/date groupings had 0 relevant events (clicks).
  Because of this, it is hard (impossible?) to know if relevant content was shown and tracked on those pages,
  adding some additional error to the analysis
*/

WITH events AS
(
  SELECT
    DatePT AS Date
    ,Platform
    ,PagePath
    --,K1
    --,K2
    --,msiteId
    ,Site
    ,EventCategory
    ,EventAction
    ,EventLabel
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
    DatePT BETWEEN '2020-01-01' AND '2020-01-31'
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
    1, 2, 3, 4, 5, 6, 7, 8--, 9, 10
), traffic AS
(
  SELECT
    DatePT AS Date
    ,Platform
    ,PagePath
    --,K1
    --,K2
    --,msiteId
    ,Site
    ,SUM(SessionCount) AS TotalSessions
    ,SUM(PageViews) AS TotalPageViews
    ,SAFE_DIVIDE(SUM(SecondsOnSite), SUM(SessionCount)) AS AvgSessionDuration
  FROM
    `dataeng-214618.PROD_Audience.traffic_*`
  WHERE
    DatePT BETWEEN '2020-01-01' AND '2020-01-31'
    AND _TABLE_SUFFIX IN ('grt', 'hl', 'mnt')
    AND
    (
      msiteId = 'None'
      OR msiteId = ''
      OR msiteID IS NULL
    )
  GROUP BY
    1, 2, 3, 4--, 5, 6, 7
  HAVING
    TotalPageviews > 0
)

SELECT
  traffic.Date
  ,traffic.Platform
  ,traffic.PagePath
  --,events.K1
  --,events.K2
  --,traffic.msiteId
  ,traffic.Site
  ,events.EventCategory
  ,events.EventAction
  ,events.EventLabel
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
  ON events.Date = traffic.Date
  AND events.Platform = traffic.Platform
  AND events.PagePath = traffic.PagePath
  --AND events.K1 = traffic.K1
  --AND events.K2 = traffic.K2
  --AND events.msiteId = traffic.msiteId
  AND events.Site = traffic.Site
ORDER BY
  1, 3 DESC