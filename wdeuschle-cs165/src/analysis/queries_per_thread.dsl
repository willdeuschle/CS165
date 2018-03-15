--
-- Timing for batching queries
--
-- Query in SQL:
--
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 12000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 110 AND col1 < 22000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 210 AND col1 < 32000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 310 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 12000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 110 AND col1 < 22000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 210 AND col1 < 32000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 310 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 12000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 110 AND col1 < 22000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 210 AND col1 < 32000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 310 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 12000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 110 AND col1 < 22000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 210 AND col1 < 32000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 310 AND col1 < 42000;
-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 42000;
--
batch_queries()
--
s1=select(db1.tbl3_batch.col1,10,12000)
s2=select(db1.tbl3_batch.col1,110,22000)
s3=select(db1.tbl3_batch.col1,210,32000)
s4=select(db1.tbl3_batch.col1,310,42000)
s5=select(db1.tbl3_batch.col1,10,42000)
s6=select(db1.tbl3_batch.col1,10,12000)
s7=select(db1.tbl3_batch.col1,110,22000)
s8=select(db1.tbl3_batch.col1,210,32000)
s9=select(db1.tbl3_batch.col1,310,42000)
s10=select(db1.tbl3_batch.col1,10,42000)
s11=select(db1.tbl3_batch.col1,10,12000)
s12=select(db1.tbl3_batch.col1,110,22000)
s13=select(db1.tbl3_batch.col1,210,32000)
s14=select(db1.tbl3_batch.col1,310,42000)
s15=select(db1.tbl3_batch.col1,10,42000)
s16=select(db1.tbl3_batch.col1,10,12000)
s17=select(db1.tbl3_batch.col1,110,22000)
s18=select(db1.tbl3_batch.col1,210,32000)
s19=select(db1.tbl3_batch.col1,310,42000)
s20=select(db1.tbl3_batch.col1,10,42000)
--
batch_execute()
