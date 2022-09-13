SELECT P.name
FROM Pokemon P, Evolution E
WHERE P.id = E.before_id
AND E.before_id > E.after_id
ORDER BY P.name;
