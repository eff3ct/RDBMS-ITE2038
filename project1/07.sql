SELECT S.name, S.cl AS MAX
FROM (
    SELECT T.name, SUM(C.level) AS cl
    FROM Trainer T, caughtPokemon C
    WHERE T.id = C.owner_id
    GROUP BY T.name
) AS S
WHERE S.cl = (
    SELECT MAX(SS.cl)
    FROM (
        SELECT T.name, SUM(C.level) AS cl
        FROM Trainer T, caughtPokemon C
        WHERE T.id = C.owner_id
        GROUP BY T.name
    ) AS SS
)
GROUP BY S.name, S.cl
ORDER BY S.name;
