SELECT MAX(CNT.c), ROUND(100 * MAX(CNT.c) / SUM(CNT.c), 2) AS PCTG
FROM (
    SELECT COUNT(P.type) AS c
    FROM Pokemon P
    GROUP BY P.type
) AS CNT;
