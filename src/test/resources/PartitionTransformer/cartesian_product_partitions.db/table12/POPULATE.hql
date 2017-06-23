INSERT OVERWRITE TABLE cartesian_product_partitions.table12 
PARTITION(part1 = ${partition:part1}, part2 = ${partition:part2})
SELECT COALESCE(T1.id, T2.id)
FROM cartesian_product_partitions.table1 T1
FULL JOIN cartesian_product_partitions.table2 T2
ON T1.id = T2.id 
WHERE T1.part1 = ${partition:part1}
  AND T2.part2 = ${partition:part2} 


