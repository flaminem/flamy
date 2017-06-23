CREATE TABLE IF NOT EXISTS less_partition_columns.table0(
  id STRING  
)
PARTITIONED BY (part1 STRING, part2 STRING)
;
