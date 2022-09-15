SELECT T.name
FROM Trainer T, caughtPokemon C
WHERE T.id = C.owner_id
AND (
    C.level = (
        SELECT MAX(CC.level)
        FROM caughtPokemon CC
    )
    OR
    C.level = (
        SELECT MIN(CC.level)
        FROM caughtPokemon CC
    )
)
GROUP BY T.name
ORDER BY T.name;
