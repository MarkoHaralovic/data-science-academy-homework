SELECT
    formatDateTime(start_of_month, '%Y-%m-%d') as month,
    MAX(mharalovic.monthly_event_activity.user_count) as max_month_count
FROM mharalovic.monthly_event_activity
GROUP BY month
ORDER BY max_month_count DESC
LIMIT 1;

-- ANSWER: 2023-03-01,3434121
