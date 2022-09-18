SELECT T.name, C.nickname
FROM Trainer T, caughtPokemon C
WHERE T.id = C.owner_id
AND T.id IN (
    SELECT owner_id
    FROM caughtPokemon
    GROUP BY owner_id
    HAVING COUNT(*) >= 3
)
AND C.level = (
    SELECT MAX(CC.level)
    FROM caughtPokemon CC
    WHERE CC.owner_id = T.id
)
GROUP BY T.name, C.nickname
ORDER BY T.name, C.nickname;
