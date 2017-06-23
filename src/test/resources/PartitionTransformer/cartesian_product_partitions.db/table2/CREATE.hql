CREATE TABLE IF NOT EXISTS cartesian_product_partitions.table2(
  id STRING  
)
PARTITIONED BY (part2 STRING)
;
