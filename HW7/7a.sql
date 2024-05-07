select
    bq.events.event_date AS ev_date,
    AVG(distinct_events_per_user) AS avg_events_per_user
from
    (select
         bq.events.event_date,
         bq.events.user_pseudo_id ,
         COUNT(DISTINCT sports.event.id) AS distinct_events_per_user
    from
        bq.events
    left join sports.event
        on bq.events.id = sports.event.id
    left join sports.sport
        on toString(sports.event.sport_id) = sports.sport.id
    where bq.events.event_date >= '20230101'
          and bq.events.event_date <= '20230530'
          and bq.events.event_name = 'open_event'
          and platform = 'IOS'
          and geo_country = 'Croatia'
          and sports.sport.name='Football'
    group by bq.events.event_date,bq.events.user_pseudo_id
    ) as day_usery_opens
GROUP BY
    ev_date
ORDER BY
    ev_date