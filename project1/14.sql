SELECT P.name
FROM Pokemon P
WHERE P.id NOT IN (
    SELECT C.pid
    FROM caughtPokemon C
)
ORDER BY P.name;
