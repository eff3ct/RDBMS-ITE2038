SELECT T.hometown, C.nickname
FROM Trainer T, caughtPokemon C
WHERE T.id = C.owner_id
AND C.level = (
    SELECT MAX(level)
    FROM caughtPokemon
    WHERE owner_id = T.id
)
GROUP BY T.hometown, C.nickname
ORDER BY T.hometown;
