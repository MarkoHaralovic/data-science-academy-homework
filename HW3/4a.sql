SELECT
    toMonday(event_date) AS week_start_date,
    dictGet('mharalovic.sport_dictionary', 'name', event_id) AS sport,
    SUM(count) AS weekly_count,
    row_number() OVER (PARTITION BY toMonday(event_date) ORDER BY SUM(count) DESC) AS rank
FROM
    mharalovic.daily_event_openings
WHERE
    event_date BETWEEN '2023-01-02' AND '2023-01-28'
GROUP BY
    week_start_date,
    event_id
ORDER BY
    week_start_date,
    weekly_count DESC;
    