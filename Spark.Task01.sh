1. Using Spark, load contents of /user/cloudera/spark/retail_db/customers. 
Find only those customers who live in CA state and save 
the subset to/user/cloudera/spark/output/task_01_out as avro with snappy compression.

var customers = sc.textFile("/user/cloudera/spark/retail_db/customers")

var customersDf = customers.map(rec => {
    var row = rec.split(',')
    (row(0).toInt, row(1) + " " + row(2), row(7))
}).toDF("customer_id", "customer_name", "customer_state")

var customersDfca = customersDf.filter(customersDf("customer_state") === "CA")

customersDfca.write.mode("overwrite").option("compression", "snappy").format("com.databricks.spark.avro").save("/user/cloudera/spark/output/task_01_out")


scala> customersDfca.show()
+-----------+---------------+--------------+
|customer_id|  customer_name|customer_state|
+-----------+---------------+--------------+
|          4|     Mary Jones|            CA|
|         14|Katherine Smith|            CA|
|         15|      Jane Luna|            CA|
|         18|   Robert Smith|            CA|
|         35|Margaret Wright|            CA|
|         40|     Mary Smith|            CA|
|         44|   Howard Smith|            CA|
|         50|       Mary Kim|            CA|
|         59|  Douglas James|            CA|
|         70|   Mary Simmons|            CA|
|         72|Frank Gillespie|            CA|
|         76|   Joseph Young|            CA|
|         89|     Sean Smith|            CA|
|        106| Lauren Freeman|            CA|
|        114|   Alice Warner|            CA|
|        115|     Mary Smith|            CA|
|        125| Mary Gallagher|            CA|
|        139| Daniel Maxwell|            CA|
|        149|Shirley Mcclain|            CA|
|        156|     Mary Smith|            CA|
+-----------+---------------+--------------+
only showing top 20 rows


scala> [cloudera@quickstart ~]$ hadoop fs -ls /user/cloudera/spark/output/task_01_out
Found 5 items
-rw-r--r--   1 cloudera cloudera          0 2019-01-19 13:05 /user/cloudera/spark/output/task_01_out/_SUCCESS
-rw-r--r--   1 cloudera cloudera       6137 2019-01-19 13:05 /user/cloudera/spark/output/task_01_out/part-r-00000-de9e980c-5e4b-43e3-ba91-76f44855dde0.avro
-rw-r--r--   1 cloudera cloudera       6078 2019-01-19 13:05 /user/cloudera/spark/output/task_01_out/part-r-00001-de9e980c-5e4b-43e3-ba91-76f44855dde0.avro
-rw-r--r--   1 cloudera cloudera       6076 2019-01-19 13:05 /user/cloudera/spark/output/task_01_out/part-r-00002-de9e980c-5e4b-43e3-ba91-76f44855dde0.avro
-rw-r--r--   1 cloudera cloudera       6660 2019-01-19 13:05 /user/cloudera/spark/output/task_01_out/part-r-00003-de9e980c-5e4b-43e3-ba91-76f44855dde0.avro








