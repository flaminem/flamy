CREATE TABLE IF NOT EXISTS more_partition_columns.table2(
  id STRING  
)
PARTITIONED BY (part1 STRING, part2 STRING)
;
