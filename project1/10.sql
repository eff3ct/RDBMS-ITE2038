SELECT P.type, P.name
FROM Pokemon P
WHERE P.id IN (
    SELECT before_id
    FROM Evolution
    UNION
    SELECT after_id
    FROM Evolution
)
ORDER BY P.type;
