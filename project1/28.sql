SELECT P.name, C.level
FROM Pokemon P, caughtPokemon C, Gym G, Trainer T
WHERE G.city = 'Sangnok City'
AND G.leader_id = T.id
AND T.id = C.owner_id
AND C.pid = P.id
ORDER BY C.level;
