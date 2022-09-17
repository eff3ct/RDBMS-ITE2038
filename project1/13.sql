SELECT P.id, P.name, P1.name, P2.name
FROM Pokemon P, Pokemon P1, Pokemon P2
WHERE P.id IN (
    SELECT before_id
    FROM Evolution
)
AND P1.id = (
    SELECT after_id
    FROM Evolution
    WHERE before_id = P.id
)
AND P2.id = (
    SELECT after_id
    FROM Evolution
    WHERE before_id = P1.id
)
ORDER BY P.id;
