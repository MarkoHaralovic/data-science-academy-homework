CREATE TABLE mharalovic.daily_event_openings(
    `event_date` Date,
    `event_id` Int32,
    `count` UInt64
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date,event_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW mharalovic.mv_daily_event_openings
TO mharalovic.daily_event_openings
AS
SELECT
    toDate(event_date) AS event_date,
    id AS event_id,
    COUNT(*) AS count
FROM
    bq.events
WHERE
    event_name = 'open_event'
GROUP BY
    event_date,
    event_id;