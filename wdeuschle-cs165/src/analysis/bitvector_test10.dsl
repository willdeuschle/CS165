-- Test Select + Fetch for bitvectors vs position vectors
--
-- SELECT col1 FROM tbl1 WHERE col1 < 20;
s1=select(db1.tbl1.col1,0,10)
f1=fetch(db1.tbl1.col1,s1)
print(f1)
