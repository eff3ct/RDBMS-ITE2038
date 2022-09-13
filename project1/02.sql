SELECT DISTINCT T.name
FROM Trainer T
WHERE T.id NOT IN (
    SELECT leader_id
    FROM Gym
)
ORDER BY T.name;
