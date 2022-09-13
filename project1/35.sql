SELECT T.name, SUM(C.level)
FROM Trainer T, caughtPokemon C
WHERE T.hometown = 'Blue City'
AND T.id = C.owner_id
AND C.pid IN (
    SELECT before_id
    FROM Evolution
    UNION
    SELECT after_id
    FROM Evolution
)
GROUP BY T.name
ORDER BY T.name;
