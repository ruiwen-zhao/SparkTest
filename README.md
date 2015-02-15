# SparkTest
Dealing big-data from HBase with Spark, using Java.

In hbase, records are kept in the following format:

RowKey|Info:carID|Info:name|Info:time
---|---|---|---
0001|5556|Honda|0214
0002|5557|Toyota|0311
0003|5556|Honda|0215
0004|5558|Buick|0412

Feb. 15: After mapping and combineByKey, we get several lists goes that:

|5556: 0214, 0215|
|---|
|5557: 0311|
|5558: 0412|
