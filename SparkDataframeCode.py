'''
--------------------------------------------------------------------------------------------
Setting up history server in local machine:
--------------------------------------------------------------------------------------------
1. Set up the following configuration parameter in spark-defaults.conf (present in SPARK_HOME/conf)
spark.eventLog.enabled             true
spark.eventLog.dir                 file:///Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/SparkBinaries/SparkHistoryLog
spark.history.fs.logDirectory      file:///Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/SparkBinaries/SparkHistoryLog

2. Start history server using script present in SPARK_HOME/sbin/start-history-server.sh
3. History server is available in localhost:18080

Executing Python code directly from PyCharm will not add entry in history server. Python file has
has to be executed using "spark-submit". Or else use the following set up.

--------------------------------------------------------------------------------------------
Setting up PyCharm so that PySpark application that is executed here is visible in Spark UI:
--------------------------------------------------------------------------------------------
1. Go to "Run"
2. Go to "Edit Configurations"
3. Set up the following "Environment Variables"
   SPARK_HOME = /Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/SparkBinaries/SparkBinary/spark-3.4.2-bin-hadoop3
   PYTHON_PATH = $SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
--------------------------------------------------------------------------------------------
'''


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("prepzee-SparkSQL-select").master('local[*]').getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# df1 = spark.read.format('csv'). \
#     option('header','true').\
#     option('inferSchema','true').\
#     load('/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Prepzee/SparkHandsOn/Dataset/Electric_Vehicle_Population_Data.csv')
#
# # print(df1.rdd.getNumPartitions())
#
# df11 = df1.groupBy('Make').count().orderBy('count')
# df12 = df11.where(col('count') > 10000)
#
# df2 = df1.select('VIN (1-10)', 'Postal Code', 'Model Year', 'Make','Electric Vehicle Type','Electric Range').\
#     where(col('Electric Range') > 100)
#
# # df2.show()
# # df12.show()
#
# df2.write.format('parquet').\
#     save('/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Prepzee/SparkHandsOn/Dataset/ev_population_parquet')

# df_parquet = spark.read.format('parquet').\
#     load('/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Prepzee/SparkHandsOn/Dataset/ev_population_parquet')
#
# df_parquet.printSchema()

'''
 |-- customer_id: string (nullable = true)
 |-- customer_unique_id: string (nullable = true)
 |-- customer_zip_code_prefix: integer (nullable = true)
 |-- customer_city: string (nullable = true)
 |-- customer_state: string (nullable = true)
'''
df1 = spark.read.format('csv'). \
       option('inferSchema', 'true').\
        option('header', 'true').\
    load('/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Prepzee/LoadingDataSampleFiles/olist_customers_dataset.csv')

df1.createOrReplaceTempView('customer')
# df1.write.saveAsTable('customer')
# spark.sql("select customer_id, customer_state from customer").show()
# df1.select('customer_id', 'customer_state').show()

# Spark execution:
# Create Dataframe/SQL on Spark --> RDDs --> executed

'''
 |-- order_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- order_status: string (nullable = true)
 |-- order_purchase_timestamp: timestamp (nullable = true)
 |-- order_approved_at: timestamp (nullable = true)
 |-- order_delivered_carrier_date: timestamp (nullable = true)
 |-- order_delivered_customer_date: timestamp (nullable = true)
 |-- order_estimated_delivery_date: timestamp (nullable = true)
'''
df2 = spark.read.format('csv'). \
    option('inferSchema', 'true'). \
    option('header', 'true'). \
    load('/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Prepzee/LoadingDataSampleFiles/olist_orders_dataset.csv')

df2.createOrReplaceTempView('order')

# df2.write.saveAsTable('orders')
# spark.sql('select order_id, order_status from orders').show()

'''
 |-- order_id: string (nullable = true)
 |-- order_item_id: integer (nullable = true)
 |-- product_id: string (nullable = true)
 |-- seller_id: string (nullable = true)
 |-- shipping_limit_date: timestamp (nullable = true)
 |-- price: double (nullable = true)
 |-- freight_value: double (nullable = true)
'''

from pyspark.sql.types import *

schema = StructType([
    StructField('order_id',StringType()),
    StructField('order_item_id',IntegerType()),
    StructField('product_id',StringType()),
    StructField('seller_id',StringType()),
    StructField('shipping_limit_date',TimestampType()),
    StructField('price',DoubleType()),
    StructField('freight_value',DoubleType())
    ])

# df3 = spark.read.format('csv'). \
#     schema(schema).\
#     load('/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Prepzee/LoadingDataSampleFiles/olist_order_items_dataset.csv')

# Narrow transformation - no shuffle - no exchange
# df3.select('product_id','price').where('price > 50.00').show()

# Join 2 dataframes - Wide Transformation

df_join1 = df1.join(df2, df1.customer_id == df2.customer_id, 'inner').\
    select(df2.order_id, df2.customer_id, 'order_status', 'order_delivered_customer_date' )
df_join1.show()

spark.sql('select c.customer_id, c.customer_zip_code_prefix, o.order_id, o.order_status from customer c, order o where c.customer_id = o.customer_id').show()

# df_join1.explain()

# df_join11 = df_join1.select('order_id', 'customer_id', 'order_status', 'order_delivered_customer_date' )

# df_join2 = df2.join(df3, df2.order_id == df3.order_id).select('product_id','price')
# df_join2.groupBy('product_id').sum('price').show()

# Exercise -
# Join 3 dataframes
# Run the same using Spark SQL
# Use groupBy and see Spark UI output


# EXPLAIN
# df3.select('product_id','price').where('price > 50.00').explain()
























