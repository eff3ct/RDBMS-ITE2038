SELECT P1.name
FROM Pokemon P1, Pokemon P2, Evolution E
WHERE P1.id = E.after_id
AND P2.id = E.before_id
AND P2.id NOT IN (
    SELECT after_id
    FROM Evolution
)
ORDER BY P1.name;
