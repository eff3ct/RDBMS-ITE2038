SELECT T.name
FROM Trainer T, caughtPokemon C1, caughtPokemon C2
WHERE T.id = C1.owner_id
AND T.id = C2.owner_id
AND C1.id <> C2.id
AND C1.pid = C2.pid
GROUP BY T.name
ORDER BY T.name;
