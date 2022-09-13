SELECT T.name, T.hometown
FROM Trainer T, Gym G, caughtPokemon C
WHERE T.id = G.leader_id
AND T.id = C.owner_id
AND C.pid IN (
    SELECT E.after_id
    FROM Evolution E, Pokemon P1, Pokemon P2
    WHERE P1.id = E.before_id
    AND P2.id = E.after_id
    AND P1.id NOT IN (
        SELECT after_id
        FROM Evolution
    )
)
GROUP BY T.name, T.hometown
ORDER BY T.name;
