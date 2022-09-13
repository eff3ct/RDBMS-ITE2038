SELECT T.name, SUM(C.level)
FROM Trainer T, caughtPokemon C, Gym G
WHERE T.id = G.leader_id
AND T.id = C.owner_id
GROUP BY T.name
ORDER BY SUM(C.level) DESC, T.name;
