SELECT P.name
FROM Pokemon P
WHERE P.id IN (
    SELECT after_id
    FROM Evolution
    INTERSECT
    SELECT before_id
    FROM Evolution
)
OR P.id IN (
    SELECT E.after_id
    FROM Evolution E, Pokemon P1, Pokemon P2
    WHERE P1.id = E.before_id
    AND P2.id = E.after_id
    AND P1.id NOT IN (
        SELECT after_id
        FROM Evolution
    )
)
ORDER BY P.id;
