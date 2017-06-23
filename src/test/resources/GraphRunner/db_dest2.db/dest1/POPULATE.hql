INSERT OVERWRITE TABLE db_dest2.dest1 PARTITION (partCol1, partCol2)
SELECT * FROM db_dest2.dest ;

INSERT OVERWRITE TABLE db_dest2.dest1 PARTITION (partCol1, partCol2)
SELECT * FROM db_dest2.dest3 ;

INSERT OVERWRITE TABLE db_dest2.dest1 PARTITION (partCol1, partCol2)
SELECT * FROM db_dest2.dest0 ;
