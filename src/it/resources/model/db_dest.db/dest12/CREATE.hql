-- DROP TABLE IF EXISTS db_dest.dest12 ;
CREATE TABLE IF NOT EXISTS db_dest.dest12
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
STORED AS SEQUENCEFILE
;
