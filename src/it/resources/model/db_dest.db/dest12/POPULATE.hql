INSERT OVERWRITE TABLE db_dest.dest12 
SELECT
  id,
  booleancol,
  tinyintcol,
  smallintcol,
  intcol,
  bigintcol,
  floatcol,
  doublecol,
  decimalcol,
  stringcol,
  varcharcol,
  timestampcol,
  datecol,
  binarycol,
  mapcol
FROM db_dest.dest1
;

INSERT OVERWRITE TABLE db_dest.dest12 
SELECT
  id,
  booleancol,
  tinyintcol,
  smallintcol,
  intcol,
  bigintcol,
  floatcol,
  doublecol,
  decimalcol,
  stringcol,
  varcharcol,
  timestampcol,
  datecol,
  binarycol,
  mapcol
FROM db_dest.dest1
;

