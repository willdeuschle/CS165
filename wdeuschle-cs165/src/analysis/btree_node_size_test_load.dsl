-- Testing node size effect on efficiency
--
-- Table tbl3 has a clustered index with col1 being the leading column.
-- The clustered index has the form of a sorted column.
-- The table also has a secondary btree index.
--
-- Loads data from: data3.csv
--
-- Create Table
create(db,"db1")
create(tbl,"tbl3",db1,4)
create(col,"col1",db1.tbl3)
create(col,"col2",db1.tbl3)
create(col,"col3",db1.tbl3)
create(col,"col4",db1.tbl3)
-- Create a clustered index on col1
create(idx,db1.tbl3.col1,sorted,clustered)
-- Create an unclustered btree index on col2
create(idx,db1.tbl3.col2,btree,unclustered)
--
--
-- Load data immediately in the form of a clustered index
-- load("/home/cs165/cs165-management-scripts/project_tests_2017/data3.csv")
-- adding this path for my tests
load("../project_tests/project_tests_1M/data3.csv")