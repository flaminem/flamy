INSERT OVERWRITE TABLE less_partition_columns.table2
SELECT
id,
part1,
part2
FROM less_partition_columns.table1
