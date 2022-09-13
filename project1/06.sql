SELECT P.name
FROM Pokemon P
WHERE P.type = 'Water'
AND (
        P.id NOT IN (
        SELECT before_id
        FROM Evolution
        UNION
        SELECT after_id
        FROM Evolution
    )
    OR
    (
        P.id IN (
            SELECT E.after_id
            FROM Pokemon P1, Evolution E
            WHERE P1.id = E.after_id
            AND P1.id NOT IN (
                SELECT before_id
                FROM Evolution
            )
        )
    )
)
ORDER BY P.name;
