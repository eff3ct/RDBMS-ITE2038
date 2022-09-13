SELECT T.name, CT.description
FROM Trainer T, Pokemon P, caughtPokemon C, City CT
WHERE T.id = C.owner_id
AND C.pid = P.id
AND P.type = 'Fire'
AND T.hometown = CT.name
GROUP BY T.name, CT.description
ORDER BY T.name;
