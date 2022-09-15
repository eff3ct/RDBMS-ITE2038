SELECT T.name, P.name, COUNT(C.owner_id)
FROM Trainer T, caughtPokemon C, Pokemon P
WHERE T.id IN (
    SELECT TT.id
    FROM Trainer TT, caughtPokemon CC, Pokemon PP
    WHERE TT.id = CC.owner_id
    AND CC.pid = PP.id
    GROUP BY TT.id
    HAVING COUNT(DISTINCT PP.type) = 1
)
AND T.id = C.owner_id
AND C.pid = P.id
GROUP BY T.name, P.name
ORDER BY T.name, P.name;
