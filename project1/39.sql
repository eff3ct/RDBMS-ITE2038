SELECT T.name, T.hometown
FROM Trainer T, caughtPokemon C1, caughtPokemon C2
WHERE T.id = C1.owner_id
AND T.id = C2.owner_id
AND C1.pid <> C2.pid
AND C2.pid IN (
    SELECT before_id
    FROM Evolution
    WHERE C1.pid = after_id
    OR C1.pid = before_id
    UNION
    SELECT after_id
    FROM Evolution
    WHERE C1.pid = after_id
    OR C1.pid = before_id
)
GROUP BY T.name, T.hometown
ORDER BY T.name;
