SELECT
    sub.name,
    RANK() OVER (PARTITION BY sub.entity ORDER BY sub.total_count DESC) as ranking
FROM (
    SELECT
        entity,
        name,
        SUM(count) as total_count
    FROM mharalovic.daily_entity_follows
    WHERE event_date BETWEEN '2023-01-16' AND '2023-01-22'
    GROUP BY entity, name
) as sub
ORDER BY sub.entity, ranking;
