CREATE TABLE mharalovic.daily_user_activity (
    `event_date` Date,
    `event_name` Date,
    `user_pseudo_id` String,
    `geo_country` LowCardinality(String),
    `platform` ENUM('ANDROID'=1,'IOS'=2,'WEB'=3),
    `event_count` Int32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date,user_pseudo_id,platform,geo_country)
SETTINGS index_granularity = 8192;

--january
INSERT INTO mharalovic.daily_user_activity
SELECT
    toDate(bq.events.event_date) as event_date,
    bq.events.user_pseudo_id,
    bq.events.geo_country,
    bq.events.platform,
    COUNT(bq.events.event_name) as event_name_count
FROM bq.events 
WHERE bq.events.event_date BETWEEN '20230101' AND '20230131'
GROUP BY event_date, bq.events.user_pseudo_id, bq.events.platform, bq.events.geo_country;

--february
INSERT INTO mharalovic.daily_user_activity
SELECT
    toDate(bq.events.event_date) as event_date,
    bq.events.user_pseudo_id,
    bq.events.geo_country,
    bq.events.platform,
    COUNT(bq.events.event_name) as event_name_count
FROM bq.events
WHERE bq.events.event_date BETWEEN '20230201' AND '20230228'
GROUP BY event_date, bq.events.user_pseudo_id, bq.events.platform, bq.events.geo_country;

--march
INSERT INTO mharalovic.daily_user_activity
SELECT
    toDate(bq.events.event_date) as event_date,
    bq.events.user_pseudo_id,
    bq.events.geo_country,
    bq.events.platform,
    COUNT(bq.events.event_name) as event_name_count
FROM bq.events
WHERE bq.events.event_date BETWEEN '20230301' AND '20230331'
GROUP BY event_date, bq.events.user_pseudo_id, bq.events.platform, bq.events.geo_country;

--april
INSERT INTO mharalovic.daily_user_activity
SELECT
    toDate(bq.events.event_date) as event_date,
    bq.events.user_pseudo_id,
    bq.events.geo_country,
    bq.events.platform,
    COUNT(bq.events.event_name) as event_name_count
FROM bq.events
WHERE bq.events.event_date BETWEEN '20230401' AND '20230430'
GROUP BY event_date, bq.events.user_pseudo_id, bq.events.platform, bq.events.geo_country;

--may
INSERT INTO mharalovic.daily_user_activity
SELECT
    toDate(bq.events.event_date) as event_date,
    bq.events.user_pseudo_id,
    bq.events.geo_country,
    bq.events.platform,
    COUNT(bq.events.event_name) as event_name_count
FROM bq.events
WHERE bq.events.event_date BETWEEN '20230501' AND '20230531'
GROUP BY event_date, bq.events.user_pseudo_id, bq.events.platform, bq.events.geo_country;

--ISSUE :[2024-03-07 16:28:58] Read timed out, server ClickHouseNode FOR MAY
--june
INSERT INTO mharalovic.daily_user_activity
SELECT
    toDate(bq.events.event_date) as event_date,
    bq.events.user_pseudo_id,
    bq.events.geo_country,
    bq.events.platform,
    COUNT(bq.events.event_name) as event_name_count
FROM bq.events
WHERE bq.events.event_date BETWEEN '20230601' AND '20230630'
GROUP BY event_date, bq.events.user_pseudo_id, bq.events.platform, bq.events.geo_country;

CREATE TABLE mharalovic.monthly_user_activity (
    `start_of_month` Date,
    `user_pseudo_id` String,
    `geo_country` LowCardinality(String),
    `platform` ENUM('ANDROID'=1,'IOS'=2,'WEB'=3),
    `event_count` Int32
)
ENGINE = MergeTree
PARTITION BY start_of_month
ORDER BY (start_of_month,user_pseudo_id,platform,geo_country)
SETTINGS index_granularity = 8192;

--january
INSERT INTO mharalovic.monthly_user_activity
SELECT
    toStartOfMonth(mharalovic.daily_user_activity.event_date) as start_of_month,
         mharalovic.daily_user_activity.user_pseudo_id,
         mharalovic.daily_user_activity.geo_country,
         mharalovic.daily_user_activity.platform,
         sum(mharalovic.daily_user_activity.event_count) as event_count
FROM mharalovic.daily_user_activity
WHERE toStartOfMonth(mharalovic.daily_user_activity.event_date)='2023-01-01'
group by start_of_month,
         mharalovic.daily_user_activity.user_pseudo_id,
         mharalovic.daily_user_activity.geo_country,
         mharalovic.daily_user_activity.platform;
--february
INSERT INTO mharalovic.monthly_user_activity
SELECT
    toStartOfMonth(mharalovic.daily_user_activity.event_date) as start_of_month,
         mharalovic.daily_user_activity.user_pseudo_id,
         mharalovic.daily_user_activity.geo_country,
         mharalovic.daily_user_activity.platform,
         sum(mharalovic.daily_user_activity.event_count) as event_count
FROM mharalovic.daily_user_activity
WHERE toStartOfMonth(mharalovic.daily_user_activity.event_date)='2023-02-01'
group by start_of_month,
         mharalovic.daily_user_activity.user_pseudo_id,
         mharalovic.daily_user_activity.geo_country,
         mharalovic.daily_user_activity.platform;
--march
INSERT INTO mharalovic.monthly_user_activity
SELECT
    toStartOfMonth(mharalovic.daily_user_activity.event_date) as start_of_month,
         mharalovic.daily_user_activity.user_pseudo_id,
         mharalovic.daily_user_activity.geo_country,
         mharalovic.daily_user_activity.platform,
         sum(mharalovic.daily_user_activity.event_count) as event_count
FROM mharalovic.daily_user_activity
WHERE toStartOfMonth(mharalovic.daily_user_activity.event_date)='2023-03-01'
group by start_of_month,
         mharalovic.daily_user_activity.user_pseudo_id,
         mharalovic.daily_user_activity.geo_country,
         mharalovic.daily_user_activity.platform;
--april
INSERT INTO mharalovic.monthly_user_activity
SELECT
    toStartOfMonth(mharalovic.daily_user_activity.event_date) as start_of_month,
         mharalovic.daily_user_activity.user_pseudo_id,
         mharalovic.daily_user_activity.geo_country,
         mharalovic.daily_user_activity.platform,
         sum(mharalovic.daily_user_activity.event_count) as event_count
FROM mharalovic.daily_user_activity
WHERE toStartOfMonth(mharalovic.daily_user_activity.event_date)='2023-04-01'
group by start_of_month,
         mharalovic.daily_user_activity.user_pseudo_id,
         mharalovic.daily_user_activity.geo_country,
         mharalovic.daily_user_activity.platform;
--may
INSERT INTO mharalovic.monthly_user_activity
SELECT
    toStartOfMonth(mharalovic.daily_user_activity.event_date) as start_of_month,
         mharalovic.daily_user_activity.user_pseudo_id,
         mharalovic.daily_user_activity.geo_country,
         mharalovic.daily_user_activity.platform,
         sum(mharalovic.daily_user_activity.event_count) as event_count
FROM mharalovic.daily_user_activity
WHERE toStartOfMonth(mharalovic.daily_user_activity.event_date)='2023-05-01'
group by start_of_month,
         mharalovic.daily_user_activity.user_pseudo_id,
         mharalovic.daily_user_activity.geo_country,
         mharalovic.daily_user_activity.platform;
--june
INSERT INTO mharalovic.monthly_user_activity
SELECT
    toStartOfMonth(mharalovic.daily_user_activity.event_date) as start_of_month,
         mharalovic.daily_user_activity.user_pseudo_id,
         mharalovic.daily_user_activity.geo_country,
         mharalovic.daily_user_activity.platform,
         sum(mharalovic.daily_user_activity.event_count) as event_count
FROM mharalovic.daily_user_activity
WHERE toStartOfMonth(mharalovic.daily_user_activity.event_date)='2023-06-01'
group by start_of_month,
         mharalovic.daily_user_activity.user_pseudo_id,
         mharalovic.daily_user_activity.geo_country,
         mharalovic.daily_user_activity.platform;

