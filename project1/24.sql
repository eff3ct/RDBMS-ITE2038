SELECT P.name
FROM Pokemon P, Trainer T, caughtPokemon C
WHERE T.hometown IN ('Sangnok City', 'Brown City')
AND T.id = C.owner_id
AND C.pid = P.id
GROUP BY P.name
ORDER BY P.name;
