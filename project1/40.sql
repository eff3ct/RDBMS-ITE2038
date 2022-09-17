SELECT T.name, CT.description
FROM Trainer T, caughtPokemon C, Pokemon P, City CT, Gym G
WHERE T.id = C.owner_id
AND G.leader_id = T.id
AND C.pid = P.id
AND P.type IN ('Fire', 'Water', 'Grass')
AND T.hometown = CT.name
AND P.id IN (
    SELECT after_id
    FROM Evolution
)
GROUP BY T.name, CT.description
ORDER BY T.name;
