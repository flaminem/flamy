INSERT OVERWRITE TABLE less_partition_columns.table1 PARTITION(part1=${partition:part1})
SELECT
id,
part2
FROM less_partition_columns.table0
