SELECT T.name
FROM Trainer AS T, (
    SELECT after_id
    FROM Evolution
    EXCEPT
    SELECT before_id
    FROM Evolution
) AS E, caughtPokemon AS C
WHERE T.id = C.owner_id
AND C.pid = E.after_id
GROUP BY T.name
ORDER BY T.name;
