CREATE TABLE IF NOT EXISTS cartesian_product_partitions.table1(
  id STRING
)
PARTITIONED BY (part1 STRING)
;
