SELECT AVG(C.level)
FROM caughtPokemon C, Pokemon P
WHERE C.pid = P.id
AND P.type = 'Water';
