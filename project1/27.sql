SELECT T.name, COALESCE(SUM(C.level), 0) AS SUM
FROM Trainer T 
LEFT OUTER JOIN caughtPokemon C 
ON C.owner_id = T.id AND C.level >= 50
JOIN Gym G
ON G.leader_id = T.id
WHERE G.leader_id = T.id
GROUP BY T.name
ORDER BY T.name;
