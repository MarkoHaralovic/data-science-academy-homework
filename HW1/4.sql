SELECT sports.sport.name as sport,
       count(*)
FROM (
       SELECT  id, event_name from bq.events
       where bq.events.event_name = 'open_event'
       and bq.events.event_date >= '20230101'
       and bq.events.event_date <= '20230131'
       and isNotNull(bq.events.id)
     ) as evnts
LEFT JOIN sports.event
         ON evnts.id = sports.event.id
LEFT JOIN sports.sport
         ON toString(sports.event.sport_id) = sports.sport.id
group by sports.sport.name