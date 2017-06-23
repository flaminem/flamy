CREATE TABLE IF NOT EXISTS cartesian_product_partitions.table12(
  id STRING  
)
PARTITIONED BY (part1 STRING, part2 STRING)
;
