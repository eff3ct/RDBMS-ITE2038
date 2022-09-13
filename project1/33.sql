SELECT SUM(C.level)
FROM caughtPokemon C, Pokemon P
WHERE C.pid = P.id
AND P.id NOT IN (
    SELECT before_id
    FROM Evolution
    UNION
    SELECT after_id
    FROM Evolution
);
