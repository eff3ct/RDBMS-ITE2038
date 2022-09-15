SELECT T.name, C.nickname, MAX(C.level)
FROM Trainer T, caughtPokemon C
WHERE T.id = C.owner_id
AND C.level = (
    SELECT MAX(CC.level)
    FROM caughtPokemon CC
    WHERE CC.owner_id = T.id
)
GROUP BY T.name, C.nickname
ORDER BY T.name, C.nickname;
