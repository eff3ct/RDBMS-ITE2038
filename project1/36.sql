SELECT SUM(C.level)
FROM caughtPokemon C, Pokemon P
WHERE C.pid = P.id
AND P.type NOT IN ('Fire', 'Grass', 'Water', 'Electric');
