INSERT OVERWRITE TABLE db_dest2.dest PARTITION (partCol1, partCol2)
SELECT * FROM db_source.source_view
