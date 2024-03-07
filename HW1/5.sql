SELECT
    sports.event.id,
    COUNT(*) AS open_count
FROM bq.events
INNER JOIN sports.event  ON bq.events.id = sports.event.id
INNER JOIN sports.tournament  ON sports.event.tournament_id = sports.tournament.id
INNER JOIN sports.uniquetournament  ON sports.tournament.uniquetournament_id = sports.uniquetournament.id
WHERE sports.tournament.name = 'HNL' AND extract(YEAR FROM sports.uniquetournament.startdate) = 2023
GROUP BY sports.event.id
ORDER BY open_count DESC
LIMIT 1;

-- ANSWER: 10396394,328107
