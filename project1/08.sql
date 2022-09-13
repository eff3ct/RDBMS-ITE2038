SELECT T.name, AVG(C.level)
FROM Trainer T, caughtPokemon C, Pokemon P
WHERE T.id = C.owner_id
AND C.pid = P.id
AND P.type IN ('Normal', 'Electric')
GROUP BY T.name
ORDER BY AVG(C.level), T.name;
