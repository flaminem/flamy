INSERT OVERWRITE TABLE more_partition_columns.table2 PARTITION(part1, part2)
SELECT
id,
${partition:part1} as part1,
part2
FROM more_partition_columns.table1
WHERE part1 = ${partition:part1}
