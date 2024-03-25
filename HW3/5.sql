CREATE TABLE mharalovic.daily_event_activity (
    `event_date` Date CODEC(DoubleDelta, LZ4),
    `event_name` LowCardinality(String) CODEC(LZ4),
    `geo_country` LowCardinality(String) CODEC(LZ4),
    `platform` LowCardinality(String) CODEC(LZ4),
    `count` UInt64 CODEC(T64, LZ4),
    `user_count` UInt64 CODEC(T64, LZ4)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, platform, geo_country, event_name)
SETTINGS index_granularity = 8192;


INSERT INTO mharalovic.daily_event_activity
SELECT
    event_date,
    event_name,
    geo_country,
    platform,
    COUNT(DISTINCT user_pseudo_id) as user_count,
    SUM(count) as count
FROM aggregations.daily_user_activity
GROUP BY GROUPING SETS(
    (event_date,event_name,geo_country),
    (event_date,event_name,platform),
    (event_date,geo_country,platform),
    (event_date,event_name),
    (event_date,geo_country),
    (event_date,platform),
    (event_date)
)


CREATE TABLE mharalovic.montly_event_activity (
    `start_of_month` Date CODEC(DoubleDelta, LZ4),
    `event_name` LowCardinality(String) CODEC(LZ4),
    `geo_country` LowCardinality(String) CODEC(LZ4),
    `platform` LowCardinality(String) CODEC(LZ4),
    `monthly_event_count` UInt32 CODEC(T64, LZ4),
    `user_count` UInt32 CODEC(T64, LZ4)
)
ENGINE = MergeTree
PARTITION BY start_of_month
ORDER BY (start_of_month,platform);


INSERT INTO mharalovic.montly_event_activity
SELECT
    event_date,
    event_name,
    geo_country,
    platform,
    COUNT(DISTINCT user_pseudo_id) as user_count,
    SUM(count) as monthly_event_count
FROM aggregations.monthly_user_activity
GROUP BY GROUPING SETS(
    (event_date,event_name,geo_country),
    (event_date,event_name,platform),
    (event_date,geo_country,platform),
    (event_date,event_name),
    (event_date,geo_country),
    (event_date,platform),
    (event_date)
);