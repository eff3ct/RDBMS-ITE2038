SELECT T.name, T.hometown
FROM Trainer T, Gym G, caughtPokemon C
WHERE T.id = G.leader_id
AND T.id = C.owner_id
AND (
    C.pid IN (
        SELECT after_id
        FROM Evolution
        EXCEPT
        SELECT before_id
        FROM Evolution
    )
    OR C.pid IN (
        SELECT P.id
        FROM Pokemon P
        WHERE P.id NOT IN (
            SELECT after_id
            FROM Evolution
            UNION
            SELECT before_id
            FROM Evolution
        )
    )
)
GROUP BY T.name, T.hometown
ORDER BY T.name;
