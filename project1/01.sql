SELECT T.name, COUNT(C.owner_id)
FROM Trainer T, caughtPokemon C
WHERE T.id = C.owner_id
GROUP BY T.name
HAVING COUNT(C.owner_id) >= 3
ORDER BY COUNT(*);
