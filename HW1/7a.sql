SELECT
    mharalovic.daily_event_activity.event_date,
    MAX(mharalovic.daily_event_activity.user_count) as max_user_count
FROM mharalovic.daily_event_activity
GROUP BY event_date
ORDER BY max_user_count DESC
LIMIT 1;

-- ANSWER: 2023-03-19,1049061
