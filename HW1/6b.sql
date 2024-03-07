CREATE TABLE mharalovic.daily_event_activity (
    `event_date` Date,
    `geo_country` LowCardinality(String),
    `platform` ENUM('ANDROID'=1,'IOS'=2,'WEB'=3,'<all>'=4),
    `daily_event_count` Int32,
    `user_count` Int32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, geo_country, platform)
SETTINGS index_granularity = 8192;

--populating daily_event_activity
--january
INSERT INTO mharalovic.daily_event_activity
SELECT
    mharalovic.daily_user_activity.event_date,
    mharalovic.daily_user_activity.geo_country,
    mharalovic.daily_user_activity.platform,
    SUM(mharalovic.daily_user_activity.event_count) AS daily_event_count,
    COUNT(DISTINCT mharalovic.daily_user_activity.user_pseudo_id) AS user_count
FROM mharalovic.daily_user_activity
WHERE mharalovic.daily_user_activity.event_date BETWEEN '2023-01-01' AND '2023-01-31'
GROUP BY
    mharalovic.daily_user_activity.event_date,
    mharalovic.daily_user_activity.platform,
    mharalovic.daily_user_activity.geo_country;

--february
INSERT INTO mharalovic.daily_event_activity
SELECT
    mharalovic.daily_user_activity.event_date,
    mharalovic.daily_user_activity.geo_country,
    mharalovic.daily_user_activity.platform,
    SUM(mharalovic.daily_user_activity.event_count) AS daily_event_count,
    COUNT(DISTINCT mharalovic.daily_user_activity.user_pseudo_id) AS user_count
FROM mharalovic.daily_user_activity
WHERE mharalovic.daily_user_activity.event_date BETWEEN '2023-02-01' AND '2023-02-28'
GROUP BY
    mharalovic.daily_user_activity.event_date,
    mharalovic.daily_user_activity.platform,
    mharalovic.daily_user_activity.geo_country;

--march
INSERT INTO mharalovic.daily_event_activity
SELECT
    mharalovic.daily_user_activity.event_date,
    mharalovic.daily_user_activity.geo_country,
    mharalovic.daily_user_activity.platform,
    SUM(mharalovic.daily_user_activity.event_count) AS daily_event_count,
    COUNT(DISTINCT mharalovic.daily_user_activity.user_pseudo_id) AS user_count
FROM mharalovic.daily_user_activity
WHERE mharalovic.daily_user_activity.event_date BETWEEN '2023-03-01' AND '2023-03-31'
GROUP BY
    mharalovic.daily_user_activity.event_date,
    mharalovic.daily_user_activity.platform,
    mharalovic.daily_user_activity.geo_country;

--april
INSERT INTO mharalovic.daily_event_activity
SELECT
    mharalovic.daily_user_activity.event_date,
    mharalovic.daily_user_activity.geo_country,
    mharalovic.daily_user_activity.platform,
    SUM(mharalovic.daily_user_activity.event_count) AS daily_event_count,
    COUNT(DISTINCT mharalovic.daily_user_activity.user_pseudo_id) AS user_count
FROM mharalovic.daily_user_activity
WHERE mharalovic.daily_user_activity.event_date BETWEEN '2023-04-01' AND '2023-04-30'
GROUP BY
    mharalovic.daily_user_activity.event_date,
    mharalovic.daily_user_activity.platform,
    mharalovic.daily_user_activity.geo_country;

--may
INSERT INTO mharalovic.daily_event_activity
SELECT
    mharalovic.daily_user_activity.event_date,
    mharalovic.daily_user_activity.geo_country,
    mharalovic.daily_user_activity.platform,
    SUM(mharalovic.daily_user_activity.event_count) AS daily_event_count,
    COUNT(DISTINCT mharalovic.daily_user_activity.user_pseudo_id) AS user_count
FROM mharalovic.daily_user_activity
WHERE mharalovic.daily_user_activity.event_date BETWEEN '2023-05-01' AND '2023-05-31'
GROUP BY
    mharalovic.daily_user_activity.event_date,
    mharalovic.daily_user_activity

--june
INSERT INTO mharalovic.daily_event_activity
SELECT
    mharalovic.daily_user_activity.event_date,
    mharalovic.daily_user_activity.geo_country,
    mharalovic.daily_user_activity.platform,
    SUM(mharalovic.daily_user_activity.event_count) AS daily_event_count,
    COUNT(DISTINCT mharalovic.daily_user_activity.user_pseudo_id) AS user_count
FROM mharalovic.daily_user_activity
WHERE mharalovic.daily_user_activity.event_date BETWEEN '2023-06-01' AND '2023-06-30'
GROUP BY
    mharalovic.daily_user_activity.event_date,
    mharalovic.daily_user_activity.platform,
    mharalovic.daily_user_activity.geo_country;


CREATE TABLE mharalovic.monthly_event_activity (
    `start_of_month` Date,
    `geo_country` LowCardinality(String),
    `platform` ENUM('ANDROID'=1,'IOS'=2,'WEB'=3,'<all>'=4),
    `monthly_event_count` Int32,
    `user_count` Int32
)
ENGINE = MergeTree
PARTITION BY start_of_month
ORDER BY (start_of_month, geo_country, platform)
SETTINGS index_granularity = 8192;

--january
INSERT INTO mharalovic.monthly_event_activity
SELECT
    mharalovic.monthly_user_activity.start_of_month,
    mharalovic.monthly_user_activity.geo_country,
    mharalovic.monthly_user_activity.platform,
    SUM(mharalovic.monthly_user_activity.event_count) AS monthly_event_count,
    COUNT(DISTINCT mharalovic.monthly_user_activity.user_pseudo_id) AS user_count
FROM mharalovic.monthly_user_activity
WHERE mharalovic.monthly_user_activity.start_of_month='2023-01-01'
GROUP BY
    mharalovic.monthly_user_activity.start_of_month,
    mharalovic.monthly_user_activity.platform,
    mharalovic.monthly_user_activity.geo_country;

--february
INSERT INTO mharalovic.monthly_event_activity
SELECT
    mharalovic.monthly_user_activity.start_of_month,
    mharalovic.monthly_user_activity.geo_country,
    mharalovic.monthly_user_activity.platform,
    SUM(mharalovic.monthly_user_activity.event_count) AS monthly_event_count,
    COUNT(DISTINCT mharalovic.monthly_user_activity.user_pseudo_id) AS user_count
FROM mharalovic.monthly_user_activity
WHERE mharalovic.monthly_user_activity.start_of_month='2023-02-01'
GROUP BY
    mharalovic.monthly_user_activity.start_of_month,
    mharalovic.monthly_user_activity.platform,
    mharalovic.monthly_user_activity.geo_country;

--march
INSERT INTO mharalovic.monthly_event_activity
SELECT
    mharalovic.monthly_user_activity.start_of_month,
    mharalovic.monthly_user_activity.geo_country,
    mharalovic.monthly_user_activity.platform,
    SUM(mharalovic.monthly_user_activity.event_count) AS monthly_event_count,
    COUNT(DISTINCT mharalovic.monthly_user_activity.user_pseudo_id) AS user_count
FROM mharalovic.monthly_user_activity
WHERE mharalovic.monthly_user_activity.start_of_month='2023-03-01'
GROUP BY
    mharalovic.monthly_user_activity.start_of_month,
    mharalovic.monthly_user_activity.platform,
    mharalovic.monthly_user_activity.geo_country;

--april
INSERT INTO mharalovic.monthly_event_activity
SELECT
    mharalovic.monthly_user_activity.start_of_month,
    mharalovic.monthly_user_activity.geo_country,
    mharalovic.monthly_user_activity.platform,
    SUM(mharalovic.monthly_user_activity.event_count) AS monthly_event_count,
    COUNT(DISTINCT mharalovic.monthly_user_activity.user_pseudo_id) AS user_count
FROM mharalovic.monthly_user_activity
WHERE mharalovic.monthly_user_activity.start_of_month='2023-04-01'
GROUP BY
    mharalovic.monthly_user_activity.start_of_month,
    mharalovic.monthly_user_activity.platform,
    mharalovic.monthly_user_activity.geo_country;

--may
INSERT INTO mharalovic.monthly_event_activity
SELECT
    mharalovic.monthly_user_activity.start_of_month,
    mharalovic.monthly_user_activity.geo_country,
    mharalovic.monthly_user_activity.platform,
    SUM(mharalovic.monthly_user_activity.event_count) AS monthly_event_count,
    COUNT(DISTINCT mharalovic.monthly_user_activity.user_pseudo_id) AS user_count
FROM mharalovic.monthly_user_activity
WHERE mharalovic.monthly_user_activity.start_of_month='2023-05-01'
GROUP BY
    mharalovic.monthly_user_activity.start_of_month,
    mharalovic.monthly_user_activity.platform,
    mharalovic.monthly_user_activity.geo_country;

--june
INSERT INTO mharalovic.monthly_event_activity
SELECT
    mharalovic.monthly_user_activity.start_of_month,
    mharalovic.monthly_user_activity.geo_country,
    mharalovic.monthly_user_activity.platform,
    SUM(mharalovic.monthly_user_activity.event_count) AS monthly_event_count,
    COUNT(DISTINCT mharalovic.monthly_user_activity.user_pseudo_id) AS user_count
FROM mharalovic.monthly_user_activity
WHERE mharalovic.monthly_user_activity.start_of_month='2023-06-01'
GROUP BY
    mharalovic.monthly_user_activity.start_of_month,
    mharalovic.monthly_user_activity.platform,
    mharalovic.monthly_user_activity.geo_country;


----------------- adding <all> into tables
INSERT INTO mharalovic.daily_event_activity
SELECT
    event_date,
    geo_country,
    '<all>' as platform,
    SUM(event_count) AS daily_event_count,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM mharalovic.daily_user_activity
GROUP BY event_date, geo_country;

INSERT INTO mharalovic.daily_event_activity
SELECT
    event_date,
    '<all>' as geo_country,
    platform,
    SUM(event_count) AS daily_event_count,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM mharalovic.daily_user_activity
GROUP BY event_date, platform;

INSERT INTO mharalovic.daily_event_activity
SELECT
    event_date,
    '<all>' as geo_country,
    '<all>' as platform,
    SUM(event_count) AS daily_event_count,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM mharalovic.daily_user_activity
GROUP BY event_date;


INSERT INTO mharalovic.monthly_event_activity
SELECT
    toStartOfMonth(event_date) as start_of_month,
    geo_country,
    '<all>' as platform,
    SUM(event_count) AS monthly_event_count,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM mharalovic.monthly_user_activity
GROUP BY start_of_month, geo_country;

INSERT INTO mharalovic.monthly_event_activity
SELECT
    toStartOfMonth(event_date) as start_of_month,
    '<all>' as geo_country,
    platform,
    SUM(event_count) AS monthly_event_count,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM mharalovic.monthly_user_activity
GROUP BY start_of_month, platform;

INSERT INTO mharalovic.monthly_event_activity
SELECT
    toStartOfMonth(event_date) as start_of_month,
    '<all>' as geo_country,
    '<all>' as platform,
    SUM(event_count) AS monthly_event_count,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM mharalovic.monthly_user_activity
GROUP BY start_of_month;
