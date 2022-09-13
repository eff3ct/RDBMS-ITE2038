SELECT T.name
FROM Trainer T, caughtPokemon C, Pokemon P
WHERE C.owner_id = T.id
AND C.pid = P.id
AND P.type = 'Psychic'
GROUP BY T.name
ORDER BY T.name;