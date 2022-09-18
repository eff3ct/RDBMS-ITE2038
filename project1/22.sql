SELECT T.name
FROM Trainer T, caughtPokemon C1, caughtPokemon C2, Pokemon P1, Pokemon P2
WHERE T.id = C1.owner_id
AND T.id = C2.owner_id
AND C1.id <> C2.id
AND C1.pid = P1.id
AND C2.pid = P2.id
AND P1.type = 'Water'
AND P2.type = 'Water'
GROUP BY T.name
ORDER BY T.name;
