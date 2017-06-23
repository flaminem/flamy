-- DROP TABLE IF EXISTS same_partitioning.source ;
CREATE TABLE IF NOT EXISTS same_partitioning.source(
  query_id STRING COMMENT "PK unique id of the query",
  start_time TIMESTAMP COMMENT "starting time", 
  end_time TIMESTAMP COMMENT "ending time (or last log time)",   
  environment STRING COMMENT "run's environment",
  schema_name STRING COMMENT "schema (database) name",
  table_name STRING COMMENT "table name",
  command STRING COMMENT "command run",
  last_status STRING COMMENT "last command status"
)
PARTITIONED BY (day STRING COMMENT "PK", project STRING COMMENT "PK flamy project")
STORED AS ORC
;
