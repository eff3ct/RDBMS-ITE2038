SELECT MAX(CNT.c), ROUND(MAX(CNT.c) / SUM(CNT.c), 2)
FROM (
    SELECT COUNT(P.type) AS c
    FROM Pokemon P
    GROUP BY P.type
) AS CNT;
