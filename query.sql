SELECT name,hour,max(high) AS max_high
FROM 
    (SELECT *,SUBSTRING(Datetime,12,2) AS hour
    FROM project3.sta9760xinliproject3)
GROUP BY  name,hour
ORDER BY  name, hour;