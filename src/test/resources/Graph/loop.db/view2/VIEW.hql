CREATE VIEW 
loop.view2
AS 
SELECT * FROM loop.view1
UNION ALL 
SELECT * FROM loop.table0
;
