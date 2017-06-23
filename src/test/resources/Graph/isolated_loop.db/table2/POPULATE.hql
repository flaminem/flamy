INSERT OVERWRITE TABLE isolated_loop.table2
SELECT * FROM isolated_loop.table1
;
