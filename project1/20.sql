SELECT COUNT(*)
FROM Pokemon P, caughtPokemon C
WHERE P.id = C.pid
AND P.type IN ('Water', 'Electric', 'Psychic');
