2. Using Spark, load customers from /user/cloudera/spark/retail_db/customers.
Concat its columns (state, city, street, each column delimited with ";") as a new column called 'address'.
Concat its columns (fname, lname, columns delimited with " ") as a new column called 'name'. 
Save the resulting table (expected result: id, name, email, password, address, zipcode)
as parquet file with any compression to /user/cloudera/spark/output/task_02_out on HDFS.

var customers = sc.textFile("/user/cloudera/spark/retail_db/customers")

var customersDf = customers.map(rec => {
    var row = rec.split(',')
    (row(0).toInt, row(1) + " " + row(2), row(3), row(4), row(7) + ";" + row(6) + ";" + row(5), row(8))
}).toDF("id", "name", "email", "password", "address", "zipcode")

customersDf.write.mode("overwrite").option("compression", "snappy").parquet("/user/cloudera/spark/output/task_02_out")


scala> customersDf.show()
+---+------------------+---------+---------+--------------------+-------+
| id|              name|    email| password|             address|zipcode|
+---+------------------+---------+---------+--------------------+-------+
|  1| Richard Hernandez|XXXXXXXXX|XXXXXXXXX|TX;Brownsville;63...|  78521|
|  2|      Mary Barrett|XXXXXXXXX|XXXXXXXXX|CO;Littleton;9526...|  80126|
|  3|         Ann Smith|XXXXXXXXX|XXXXXXXXX|PR;Caguas;3422 Bl...|  00725|
|  4|        Mary Jones|XXXXXXXXX|XXXXXXXXX|CA;San Marcos;832...|  92069|
|  5|     Robert Hudson|XXXXXXXXX|XXXXXXXXX|PR;Caguas;10 Crys...|  00725|
|  6|        Mary Smith|XXXXXXXXX|XXXXXXXXX|NJ;Passaic;3151 S...|  07055|
|  7|    Melissa Wilcox|XXXXXXXXX|XXXXXXXXX|PR;Caguas;9453 Hi...|  00725|
|  8|       Megan Smith|XXXXXXXXX|XXXXXXXXX|MA;Lawrence;3047 ...|  01841|
|  9|        Mary Perez|XXXXXXXXX|XXXXXXXXX|PR;Caguas;3616 Qu...|  00725|
| 10|     Melissa Smith|XXXXXXXXX|XXXXXXXXX|VA;Stafford;8598 ...|  22554|
| 11|      Mary Huffman|XXXXXXXXX|XXXXXXXXX|PR;Caguas;3169 St...|  00725|
| 12| Christopher Smith|XXXXXXXXX|XXXXXXXXX|TX;San Antonio;55...|  78227|
| 13|      Mary Baldwin|XXXXXXXXX|XXXXXXXXX|PR;Caguas;7922 Ir...|  00725|
| 14|   Katherine Smith|XXXXXXXXX|XXXXXXXXX|CA;Pico Rivera;56...|  90660|
| 15|         Jane Luna|XXXXXXXXX|XXXXXXXXX|CA;Fontana;673 Bu...|  92336|
| 16|     Tiffany Smith|XXXXXXXXX|XXXXXXXXX|PR;Caguas;6651 Ir...|  00725|
| 17|     Mary Robinson|XXXXXXXXX|XXXXXXXXX|MI;Taylor;1325 No...|  48180|
| 18|      Robert Smith|XXXXXXXXX|XXXXXXXXX|CA;Martinez;2734 ...|  94553|
| 19|Stephanie Mitchell|XXXXXXXXX|XXXXXXXXX|PR;Caguas;3543 Re...|  00725|
| 20|        Mary Ellis|XXXXXXXXX|XXXXXXXXX|NJ;West New York;...|  07093|
+---+------------------+---------+---------+--------------------+-------+
only showing top 20 rows


[cloudera@quickstart ~]$ hadoop fs -ls /user/cloudera/spark/output/task_02_out
Found 7 items
-rw-r--r--   1 cloudera cloudera          0 2019-01-19 11:13 /user/cloudera/spark/output/task_02_out/_SUCCESS
-rw-r--r--   1 cloudera cloudera        641 2019-01-19 11:13 /user/cloudera/spark/output/task_02_out/_common_metadata
-rw-r--r--   1 cloudera cloudera       3834 2019-01-19 11:13 /user/cloudera/spark/output/task_02_out/_metadata
-rw-r--r--   1 cloudera cloudera      63937 2019-01-19 11:13 /user/cloudera/spark/output/task_02_out/part-r-00000-80cc4ce5-0fb4-470e-b27a-93dafc1170a7.gz.parquet
-rw-r--r--   1 cloudera cloudera      63777 2019-01-19 11:13 /user/cloudera/spark/output/task_02_out/part-r-00001-80cc4ce5-0fb4-470e-b27a-93dafc1170a7.gz.parquet
-rw-r--r--   1 cloudera cloudera      63713 2019-01-19 11:13 /user/cloudera/spark/output/task_02_out/part-r-00002-80cc4ce5-0fb4-470e-b27a-93dafc1170a7.gz.parquet
-rw-r--r--   1 cloudera cloudera      63623 2019-01-19 11:13 /user/cloudera/spark/output/task_02_out/part-r-00003-80cc4ce5-0fb4-470e-b27a-93dafc1170a7.gz.parquet


