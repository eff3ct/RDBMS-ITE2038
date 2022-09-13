SELECT T.name, COUNT(C.id)
FROM Trainer T, caughtPokemon C, Pokemon P
WHERE T.id = C.owner_id
AND C.pid = P.id
GROUP BY T.name
HAVING COUNT(DISTINCT P.type) = 2
ORDER BY T.name;
