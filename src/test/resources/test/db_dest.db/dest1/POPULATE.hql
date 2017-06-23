INSERT OVERWRITE TABLE db_dest.dest1 PARTITION (partCol1, partCol2)
SELECT * FROM db_dest.dest
