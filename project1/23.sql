SELECT P.name
FROM Pokemon P
WHERE P.name LIKE 'A%'
OR P.name LIKE 'E%'
OR P.name LIKE 'I%'
OR P.name LIKE 'O%'
OR P.name LIKE 'U%'
ORDER BY P.name;
