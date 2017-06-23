-- DROP TABLE IF EXISTS db_source.right_table ;
CREATE TABLE IF NOT EXISTS db_source.right_table(
  col1 INT,
  col2 INT,
  col3 INT
) 
PARTITIONED BY (partCol1 STRING, partCol2 STRING)
STORED AS ORC
;
