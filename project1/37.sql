SELECT C.pid, C.nickname
FROM caughtPokemon C, Trainer T, Pokemon P
WHERE C.owner_id = T.id
AND T.hometown = 'Blue City'
AND C.pid = P.id
AND P.type = 'Water'
ORDER BY C.pid, C.nickname;
