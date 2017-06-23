INSERT OVERWRITE TABLE more_partition_columns.table1 PARTITION(part1)
SELECT
id,
part2,
part1
FROM more_partition_columns.table0
