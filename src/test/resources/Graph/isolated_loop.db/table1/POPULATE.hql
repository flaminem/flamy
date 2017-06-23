INSERT OVERWRITE TABLE isolated_loop.table1 
SELECT * FROM isolated_loop.view2
;
