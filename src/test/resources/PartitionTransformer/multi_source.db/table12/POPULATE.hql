INSERT OVERWRITE TABLE multi_source.table12 PARTITION(part1 = ${partition:part1}, part2 = ${partition:part2})
SELECT
T2.id
FROM multi_source.table2 T2
JOIN multi_source.table1 T1
ON T1.id = T2.id AND T1.part1 = ${partition:part1} AND T1.part2 = ${partition:part2}
WHERE T2.part1 = ${partition:part1} AND T2.part2 = ${partition:part2}


