SELECT P.type
FROM Pokemon P
WHERE P.id IN (
    SELECT before_id
    FROM Evolution
    UNION
    SELECT after_id
    FROM Evolution
)
AND P.type IN (
    SELECT P2.type
    FROM Pokemon P2
    WHERE P2.id IN (
        SELECT before_id
        FROM Evolution
        UNION
        SELECT after_id
        FROM Evolution
    )
    GROUP BY P2.type
    HAVING COUNT(P2.type) >= 3
)
GROUP BY P.type
ORDER BY P.type;
