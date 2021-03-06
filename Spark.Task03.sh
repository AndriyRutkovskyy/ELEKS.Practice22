3. Using Spark, find how many orders were made in each state
(the input data for this task is in /user/cloudera/spark/retail_db).
Expected result: state, total_orders. Sort the results by total_orders in descending order, and by state in ascending.
Save the result as Hive table called state_orders in default database as datafile delimited by tabs.

# Hive init start
import org.apache.spark.sql.hive.HiveContext
import sqlContext.implicits._

val hiveObj = new HiveContext(sc)

hiveObj.sql("use default")
# Hive init end

var customers = sc.textFile("/user/cloudera/spark/retail_db/customers")

var orders = sc.textFile("/user/cloudera/spark/retail_db/orders")

var customersDf = customers.map(rec => {
    var row = rec.split(',')
    (row(0).toInt,  row(7))
}).toDF("customer_id", "state")

var ordersDf = orders.map(rec => {
    var row = rec.split('@')
    (row(0).toInt, row(2).toInt)
}).toDF("order_id", "order_customer_id")

var joinOrdCustState = ordersDf.join(customersDf, ordersDf("order_customer_id") === customersDf("customer_id"))

var ordersInfo = joinOrdCustState.groupBy($"state").agg(count($"order_id").as("total_orders"))

ordersInfo.sort($"total_orders".desc, $"state".asc)

ordersInfo.write.format("orc").option("delimiter", "\t").saveAsTable("default.state_orders")

# ordersInfo.write.partitionBy("state").option("delimiter", "\t").format("parquet").saveAsTable("default.state_orders")
# ordersInfo.write.partitionBy("state").option("delimiter", "\t").format("hive").saveAsTable("default.state_orders")

# scala> ordersInfo.write.mode("overwrite").format("orc").saveAsTable("default.new_res6")

# import org.apache.spark.sql.hive.HiveContext
# val sqlContext = new HiveContext(sc)
# sqlContext.sql("use default")
# resOrdered.write.format("orc").saveAsTable("default.state_orders")

# ordersInfo.write.option("delimiter", "\t").saveAsTable("default.state_orders");

scala> ordersInfo.show()
+-----+------------+                                                            
|state|total_orders|
+-----+------------+
|   MT|          32|
|   TN|         607|
|   NC|         803|
|   ND|          72|
|   AL|          13|
|   TX|        3442|
|   NJ|        1259|
|   NM|         391|
|   AR|          65|
|   NV|         540|
|   HI|         478|
|   AZ|        1156|
|   NY|        4331|
|   UT|         357|
|   OH|        1575|
|   OK|          84|
|   IA|          24|
|   OR|         646|
|   VA|         738|
|   ID|          67|
+-----+------------+
only showing top 20 rows


