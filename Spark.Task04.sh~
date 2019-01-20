4. Find how much money every customer spent. 
Expected result: customer_id, customer_name (first + last), total_expenses. 
Save results as json with gzip compression.

var customers = sc.textFile("/user/cloudera/spark/retail_db/customers")

var orders = sc.textFile("/user/cloudera/spark/retail_db/orders")

var order_items = sc.textFile("/user/cloudera/spark/retail_db/order_items")

var customersDf = customers.map(rec => {
    var row = rec.split(',')
    (row(0).toInt, row(1) + " " + row(2))
}).toDF("customer_id", "customer_name")

var ordersDf = orders.map(rec => {
    var row = rec.split('@')
    (row(0).toInt, row(2).toInt)
}).toDF("order_id", "order_customer_id")

var order_itemsDf = order_items.map(rec => {
    var row = rec.split(',')
    (row(0), row(1), row(4))
}).toDF("order_item_id", "order_item_order_id", "order_item_subtotal")

var join1 = ordersDf.join(order_itemsDf, ordersDf("order_id") === order_itemsDf("order_item_order_id"))

var res = join1.join(customersDf, join1("order_customer_id") === customersDf("customer_id")).groupBy($"customer_id", $"customer_name").agg(round(sum($"order_item_subtotal"), 2).as("total_expenses"))

res.toJSON.saveAsTextFile("/user/cloudera/spark/output/customers_report", classOf[org.apache.hadoop.io.compress.GzipCodec])


[cloudera@quickstart ~]$ hadoop fs -ls /user/cloudera/spark/output/customers_report
Found 201 items
-rw-r--r--   1 cloudera cloudera          0 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/_SUCCESS
-rw-r--r--   1 cloudera cloudera        993 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00000.gz
-rw-r--r--   1 cloudera cloudera       1051 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00001.gz
-rw-r--r--   1 cloudera cloudera       1039 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00002.gz
-rw-r--r--   1 cloudera cloudera        995 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00003.gz
-rw-r--r--   1 cloudera cloudera       1021 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00004.gz
-rw-r--r--   1 cloudera cloudera       1007 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00005.gz
-rw-r--r--   1 cloudera cloudera       1040 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00006.gz
-rw-r--r--   1 cloudera cloudera       1009 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00007.gz
-rw-r--r--   1 cloudera cloudera        988 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00008.gz
-rw-r--r--   1 cloudera cloudera        970 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00009.gz
-rw-r--r--   1 cloudera cloudera       1020 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00010.gz
-----------------------------------------------------------------------------------------------------------------------
-rw-r--r--   1 cloudera cloudera       1093 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00197.gz
-rw-r--r--   1 cloudera cloudera        969 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00198.gz
-rw-r--r--   1 cloudera cloudera       1043 2019-01-20 12:41 /user/cloudera/spark/output/customers_report/part-00199.gz

