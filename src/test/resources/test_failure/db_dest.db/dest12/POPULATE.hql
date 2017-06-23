INSERT OVERWRITE TABLE db_dest.dest12 PARTITION (partCol1, partCol2)
SELECT * FROM db_dest.dest1 ;

INSERT OVERWRITE TABLE db_dest.dest12 PARTITION (partCol1, partCol2)
SELECT * FROM db_dest.dest2 ;
