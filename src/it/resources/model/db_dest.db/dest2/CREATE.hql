-- DROP TABLE IF EXISTS db_dest.dest2 ;
CREATE TABLE IF NOT EXISTS db_dest.dest2
(
id INT,
booleanCol BOOLEAN,
tinyintCol TINYINT,
smallintCol SMALLINT,
intCol INT,
bigintCol BIGINT,
floatCol FLOAT,
doubleCol DOUBLE,
decimalCol DECIMAL,
stringCol STRING,
varcharCol VARCHAR(255),
timestampCol TIMESTAMP,
dateCol DATE,
binaryCol BINARY,
mapCol MAP<STRING,STRING>
) 
PARTITIONED BY (partCol1 STRING, partCol2 STRING)
STORED AS SEQUENCEFILE
;
