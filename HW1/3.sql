--Write an SQL query that gives the number of users that opened Buzzer Feed?
SELECT count(DISTINCT bq.events.user_pseudo_id)
FROM bq.events
WHERE bq.events.event_name = 'drawer_action' and
      bq.events.item_name ='Buzzer Feed';

-- answer:  19261