from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('PySpark_MySQL-to-Redshift').getOrCreate()

df1_mysql = spark.read.format('jdbc').option('url', 'jdbc:mysql://xxx:3306'). \
    option('dbtable', 'mydb.order_items').option('user', 'xxxx').option('password', 'xxxx'). \
    load()

df2_mysql = spark.read.format('jdbc').option('url', 'jdbc:mysql://xxxxx:3306'). \
    option('dbtable', 'mydb.products').option('user', 'xxxx').option('password', 'xxxxx'). \
    load()

df_joined = df1_mysql.join(df2_mysql, df1_mysql.product_id == df2_mysql.product_id, 'inner').\
    select(df2_mysql.product_id, df2_mysql.product_category_name, df2_mysql.product_photos_qty, df1_mysql.price)

df_redshift = df_joined.groupBy('product_id','product_category_name','product_photos_qty').sum('price')

url = "xxxxx"

df_redshift.write.format('io.github.spark_redshift_community.spark.redshift').\
    option('url', url).option('dbtable','public.product_sold_details'). \
    option('tempdir','s3://xxxx/'). \
    option('aws_iam_role','xxxxx'). \
    save()