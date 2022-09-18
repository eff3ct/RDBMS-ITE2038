SELECT T.hometown, C.nickname
FROM Trainer T, CaughtPokemon C
WHERE T.id = C.owner_id
AND C.level = (
    SELECT MAX(CC.level)
    FROM CaughtPokemon CC, Trainer TT
    WHERE TT.id = CC.owner_id
    AND TT.hometown = T.hometown
)
ORDER BY T.hometown;
