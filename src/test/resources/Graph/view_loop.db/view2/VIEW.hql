CREATE VIEW view_loop.view2
AS 
SELECT * FROM view_loop.view1
UNION ALL 
SELECT * FROM view_loop.table0
;
