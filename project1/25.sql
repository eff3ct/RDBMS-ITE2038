SELECT T.name, P.type, COUNT(C.owner_id)
FROM Trainer T, Pokemon P, caughtPokemon C
WHERE T.id = C.owner_id
AND C.pid = P.id
GROUP BY T.name, P.type
ORDER BY T.name, P.type;
