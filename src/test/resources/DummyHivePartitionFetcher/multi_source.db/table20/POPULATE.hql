INSERT OVERWRITE TABLE multi_source.table20 PARTITION(part1 = ${partition:part1}, part2 = ${partition:part2})
SELECT
T0.id
FROM multi_source.table2 T2
JOIN multi_source.table0 T0
ON T0.id = T2.id 
WHERE T2.part1 = ${partition:part1} AND T2.part2 = ${partition:part2}
AND T0.part1 = ${partition:part1} AND T0.part2 = ${partition:part2}

