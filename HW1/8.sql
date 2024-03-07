SELECT
    follows_per_player.geo_country,
    follows_per_player.name,
    follows_per_player.followers
FROM (
    SELECT
        bq.events.geo_country,
        bq.events.name,
        COUNT(*) as followers
    FROM bq.events
    INNER JOIN sports.player
        ON bq.events.name = sports.player.name
    WHERE bq.events.event_name = 'follow_player'
        AND toYear(toDate(bq.events.event_date)) = 2023
        AND toMonth(toDate(bq.events.event_date)) in (1, 2, 3)
    GROUP BY bq.events.geo_country, bq.events.name
) as follows_per_player
INNER JOIN (
    SELECT
        geo_country,
        MAX(followers) as max_followers
    FROM (
        SELECT
            bq.events.geo_country,
            COUNT(*) as followers
        FROM bq.events
        INNER JOIN sports.player
            ON bq.events.name = sports.player.name
        WHERE bq.events.event_name = 'follow_player'
            AND toYear(toDate(bq.events.event_date)) = 2023
            AND toMonth(toDate(bq.events.event_date)) in (1, 2, 3)
        GROUP BY bq.events.geo_country, bq.events.name
    ) grouped
    GROUP BY geo_country
) as max_followers_per_country
ON follows_per_player.geo_country = max_followers_per_country.geo_country
   AND follows_per_player.followers = max_followers_per_country.max_followers
ORDER BY follows_per_player.geo_country;

/*
Answer:
Albania,Cristiano Ronaldo,241
Austria,Cristiano Ronaldo,528
Bosnia & Herzegovina,Renato Erceg,1347
Bulgaria,Cristiano Ronaldo,624
Croatia,Renato Erceg,7122
Germany,Cristiano Ronaldo,2129
Hungary,Cristiano Ronaldo,706
Italy,Danilo,4935
Kosovo,Cristiano Ronaldo,271
Montenegro,Marquinhos,294
North Macedonia,Cristiano Ronaldo,98
Romania,Cristiano Ronaldo,574
Serbia,Aleksandar Mitrović,1866
Slovenia,Marquinhos,315
*/