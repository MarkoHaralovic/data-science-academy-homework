CREATE TABLE mharalovic.daily_entity_follows (
    `event_date` Date,
    `entity` ENUM('league' = 1, 'player' = 2, 'team' = 3),
    `name` String,
    `count` UInt64
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date,entity,name)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW mharalovic.mv_daily_entity_follows
TO mharalovic.daily_entity_follows
AS
SELECT
    toDate(event_date) as event_date,
    multiIf(
        event_name = 'follow_league','league',
        event_name = 'follow_player','player',
        event_name = 'follow_team','team',
        ''
    ) as entity,
    multiIf(
        event_name = 'follow_league', dictGet('mharalovic.tournament_dictionary', 'name', toUInt64(id)),
        event_name = 'follow_player', dictGet('mharalovic.player_dictionary', 'name', toUInt64(id)),
        event_name = 'follow_team', dictGet('mharalovic.team_dictionary', 'name', toUInt64(id)),
        ''
    ) as name,
    count(*) as count
FROM bq.events
where event_name in ('follow_team', 'follow_player', 'follow_league')
GROUP BY event_date, entity,name;
