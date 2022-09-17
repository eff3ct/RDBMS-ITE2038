SELECT T.name, T.hometown
FROM Trainer T, caughtPokemon C1, caughtPokemon C2
WHERE T.id = C1.owner_id
AND T.id = C2.owner_id
AND C1.pid <> C2.pid
AND C2.pid IN (
    SELECT E1.before_id
    FROM Evolution E1, Evolution E2
    WHERE E1.after_id = C1.pid
    OR (
        E1.after_id = E2.before_id
        AND E2.after_id = C1.pid
    )
    OR (
        E2.after_id = E1.before_id
        AND E2.before_id = C1.pid
    )
    UNION
    SELECT E1.after_id
    FROM Evolution E1, Evolution E2
    WHERE E1.before_id = C1.pid
    OR (
        E1.before_id = E2.after_id
        AND E2.before_id = C1.pid
    )
    OR (
        E2.before_id = E1.after_id
        AND E2.after_id = C1.pid
    )
)
GROUP BY T.name, T.hometown
ORDER BY T.name;
