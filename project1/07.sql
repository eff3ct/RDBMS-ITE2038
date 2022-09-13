SELECT T.name, MAX(C.level)
FROM Trainer AS T, caughtPokemon AS C
WHERE T.id = C.owner_id
AND C.level = (
    SELECT MAX(CC.level)
    FROM caughtPokemon AS CC
)
GROUP BY T.name
ORDER BY T.name;
