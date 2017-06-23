CREATE TABLE IF NOT EXISTS less_partition_columns.table1(
  id STRING,
  part2 STRING
)
PARTITIONED BY (part1 STRING)
;
