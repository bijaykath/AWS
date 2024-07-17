# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SQLContext
# from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

# # Lets see SparkContext in a little bit more detail :--
# conf = SparkConf().setMaster('local')
# sc = SparkContext(conf=conf)
# sc2 = SparkContext(conf=conf)
# sql = SQLContext(sc)

# # lets see the SparkContext object
# print(">> SparkContext Object 1  : ",sc)
# print(">> SparkContext Object 2  : ",sc2) # can be solved by setting spark.driver.allowMultipleContexts to 'true'
# # print(">> SQLContext Object      : ",sql)
# print(">> StreamingContext Object: ",stream)
# print("SparkContext Object: ",sc2)


#-----------------------------------------------------------------------------------------------
'''
What is SparkSession?
SparkSession = SparkContext + SQLContext + HiveContext + StreamingContext
SparkSession can be created from an existing SparkContext using SQLContext
'''
#-----------------------------------------------------------------------------------------------
# ss = sql.sparkSession # From PREVIOUS SPARKCONTEXT
# ss = SparkSession.builder.appName('Intellipaat-Dataframes').config('spark.sql.join.preferSortMergeJoin','True').master('local').getOrCreate()
# ss = SparkSession.builder.appName('Intellipaat-Dataframes').master('local').getOrCreate()
# ss = SparkSession.builder.master('local').getOrCreate()

# ss2 = ss.newSession()
# ss3 = ss.newSession()

# print("1st SparkSession: ",ss)
# print("2nd SparkSession: ",ss2)
# print("3rd SparkSession: ",ss3)
# print("SparkContext of ss {} ".format(ss.sparkContext))
# print("SparkContext of ss {} ".format(ss2.sparkContext))
# print("SparkContext of ss {} ".format(ss3.sparkContext))


#-----------------------------------------------------------------------------------------------
'''
Reading/Writing from/to Data Sources 
CSV     > ss.read.format('csv').option().option().schema(<schema-name>).load(<file>)
          ss.write.format('csv').mode(<write-mode-options>).option().option().save(<file>)
           
Parquet > ss.read.format('parquet').option().option().load(<file>) # SCHEMA IS NOT NEEDED
          ss.write.format('parquet').option().mode(<write-mode-options>).save(<file>)
          
JSON    > ss.read.format('json').option().option().schema(<schema-name>).load(<file>)
          ss.write.format('json').option().mode(<write-mode-options>).save(<file>)
          
ORC     > ss.read.format('orc').option().option().load(<file>) # SCHEMA IS NOT NEEDED
          ss.write.format('orc').option().mode(<write-mode-options>).save(<file>)
          
write modes = "append" | "overwrite" | "errorIfExists" | "ignore"

** Data can be read from JDBC sources too.
'''
#-----------------------------------------------------------------------------------------------
# ss = SparkSession.builder.appName('intellipaat-sparksession').master('local').getOrCreate()
# input_file_csv = '/Users/......../........../TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/emp_data_ORIG.csv'
# input_file_csv = '/Users/........./............./TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/emp_data_wo_header.csv'
# df1 = ss.read.format('csv').option('header','true').load(input_file_csv)
# df1.show()

# input_file_parquet = '/Users/........./HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/covid-19_patients_data.parquet/part-00000-cffc2fa1-1dd0-4b68-8005-e08f9806ee19-c000.snappy.parquet'
# df1 = ss.read.format('parquet').load(input_file_parquet) # No need to provide any schema
# df1.printSchema()
# df1.show()

# input_file_json = '/Users/........../............/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_information.json'
# input_file_json = '/Users/........../.........../TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/restaurants.json'
# df1 = ss.read.format('json').option('inferSchema','true').load(input_file_json)
# df1.show()
# df1.printSchema()

# df1 = sql.read.format('json').option('inferSchema','true').load(input_file_json)
# df1.show()

# df1 = ss.read.format('json').option('inferSchema','true').load(input_file_json)
# df1.printSchema()

#-----------------------------------------------------------------------------------------------
'''
Spark Data Types (has to be imported from pyspark.sql.types)
- ByteType() > 1byte 
- ShortType() > 2bytes
- IntegerType()
- LongType() > 8bytes
- FloatType(), DecimalType()
- StringType()
- BinaryType(), BooleanType()
- TimestampType() > Python type datetime.datetime
- DateType() > Python type datetime.date
- ArrayType(<elementType>) > list, tuple (elementType should be from above list)
- MapType(<keyType>,<valueType>) > Python dictionary
- StructType() > To define the entire schema
- StructField() > To define individual fields  

Structure to define schema:
StructType([ StructField(), StructField(), StructField() ])

To check the schema use printSchema().
'''
#-----------------------------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType


# input_restaurant_file = '/Users/........../........../TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/restaurants.json'
# input_emp_file = '/Users/.........../.............../TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/emp_data_ORIG.csv'

# emp_data_ORIGmp_schema = StructType(
#     [
#         StructField('dept_id', IntegerType(), True),
#         StructField('first_name', StringType(), True),
#         StructField('last_name', StringType(), True),
#         StructField('email', StringType(), True),
#         StructField('role', StringType(), True)
#     ]
# )
#
# empDf = ss.read.format('csv').schema(emp_schema).load((input_emp_file))
# empDf.printSchema()
# empDf.show()

# restaurant_schema = StructType(
#     [
#         StructField("address",StructType([StructField("building",StringType(),True),
#                                           StructField("coord",ArrayType(FloatType()),True),
#                                           StructField("street",StringType(),True),
#                                           StructField("zipcode",StringType(),True)]),
#                      True),
#         StructField("borough",StringType(),False),
#         StructField("cuisine",StringType(),False),
#         StructField("grades",ArrayType(StructType([StructField("date",StructType([StructField("$date",IntegerType(),False)]),False),
#                                                    StructField("grade",StringType(),False),
#                                                    StructField("score",IntegerType(),False)]))
#                      ,False),
#         StructField("name",StringType(),False),
#         StructField("restaurant_id",IntegerType(),False)
#     ]
# )

# df1 = ss.read.format('json').schema(restaurant_schema).load(input_restaurant_file)
# df1 = ss.read.format('json').option('inferSchema','true').load(input_restaurant_file)
# df1 = ss.read.format('json').load(input_restaurant_file).schem(schema) # Will give error
# df1.printSchema()
# df1.show()



#-----------------------------------------------------------------------------------------------
'''
Convert RDD to Dataframe
- createDataFrame(<rdd>,schema=<myschema>)
  1. Create an RDD
 *2. Convert it to type Row, Tuple, List etc.
  3. Define schema
  3. Apply createDataFrame method on the RDD created in step 2.
  Note: RDD need to have type Row, Tuple, List, Int, Boolean, Pandas DataFrame. If RDD is 
        created from CSV/JSON files then it has to be transformed to contain new RDD with
        above data types. Then the new RDD can be converted to a DataFrame.
          
- toDF(<list_of_column_names>) 
  Note: RDD has to be passed with type Row, Tuple, List, Int, Boolean, just the way it is
        done with 'createDataFrame'. The column names have to be passed as a LIST in the 
        argument of toDF. If we do not pass List of column names then the column names would 
        be numbered as _1, _2, _3 and so on.
        
- From an existing SparkContext using SQLContext
  df = sql.read.format('')...
'''
#-----------------------------------------------------------------------------------------------
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
# input_file_csv = '/Users/........../.............../TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/emp_data.csv'
# ss = SparkSession.builder.appName('PySpark-DataFrame').master('local').getOrCreate()
#
# from pyspark import SparkContext, SparkConf
# conf = SparkConf().setMaster('local')
# sc = SparkContext(conf=conf)

## Note: Create SparkContext from SparkSession >>>
# sc = ss.sparkContext

# schema = StructType(
#     [
#         StructField("dept_id", IntegerType(), False),
#         StructField("first_name", StringType(), False),
#         StructField("last_name", StringType(), False),
#         StructField("email", StringType(), False),
#         StructField("role", StringType(), False),
#     ]
# )
#
# def convert_to_list(x):
#     x = x.split(',')
#     # return int(x[0]),x[1],x[2],x[3],x[4]
#     return int(x[0]),x[1],x[2],x[3],x[4]
#
# inputRdd1 = sc.textFile(input_file_csv)
# inputRdd2 = inputRdd1.map(lambda x: convert_to_list(x))
# inputDf = ss.createDataFrame(inputRdd2,schema=schema)
# inputDf.show()

# inputDf2 = inputRdd2.toDF(["deptid","firstname","lastname","email","role"])
# inputDf2 = inputRdd2.toDF(["id"])
# inputDf2.show()


#-----------------------------------------------------------------------------------------------
# col, column function
# Note: Please install pyspark-stubs to use col and column functions. This is needed for PyCharm
#       IDE as these functions are resolved at runtime.
#-----------------------------------------------------------------------------------------------
from pyspark.sql.functions import col, column

# input_jpmc_file = '/Users/........../............./TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/JPMC_Bank_Database.csv'
# df1 = ss.read.format('csv').option('header','true').load(input_jpmc_file)

# df1.show()
# print(df1.columns)
# df1.printSchema()

# df2 = df1.select("Branch_Name").filter(col("Branch_Name").startswith("s".upper()))
# df2.show()
# df1.select("Branch_Name","2010_Deposits").filter(col("2010_Deposits").cast(IntegerType()) > 1000000).show(5)

# df2 = df1.select("Branch_Name","2010_Deposits")
# df2.show()
# df1.select("Branch_Name","Established_Date").filter(df1["Branch_Name"].startswith("JP")).show()


#-----------------------------------------------------------------------------------------------
'''
DataFrame Transformations
- select
- limit
- filter | where
- orderBy
- sort
- groupBy
- union
- join
- agg
'''
#-----------------------------------------------------------------------------------------------
# input_jpmc_file = '/Users/.........../............../TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/JPMC_Bank_Database.csv'
# df1 = ss.read.format('csv').option('header','true').load(input_jpmc_file)

# df1.printSchema()

# SELECT
#--------
# df1.select('Institution_Name','Branch_Name','Established_Date').show()
# df2 = df1.select('Institution_Name','Branch_Name','Established_Date').show()

# for i in df2.take(75):
#     print(i)

# FILTER | WHERE
#----------------
# df2 = df1.select('Institution_Name','Branch_Name','Established_Date')       # dfrdd2 = dfrdd1.map(lambda x: (x[0],x[4],x[10]))
# df3 = df2.filter(col("Branch_Name").startswith("J"))                        # dfrdd3 = dfrdd2.filter(lambda x: starts_with_j(x))
# df3 = df2.where(col("Branch_Name").startswith("J"))
# df3.show()
# print(df3.count())

# ORDERBY | SORT | LIMIT
#------------------------
# df1.printSchema()
# df2 = df1.select("Branch_Name", "2010_Deposits","Established_Date")         # dfrdd2 = dfrdd1.map(lambda x: (x[0],x[4],x[10]))
# df3 = df2.orderBy(col("2010_Deposits").cast(IntegerType()).desc())          # dfrdd3 = dfrdd1.sortBy(lambda x: x[1],ascending=False)
# df3 = df2.sort(col("2010_Deposits").cast(IntegerType()).desc())
# df4 = df3.limit(5)
# df4.show()
# df2 = df1.select('Branch_Name').distinct().limit(5).show()
# df2 = df1.select('Branch_Name').orderBy('Branch_Name').show()
# df2.show()


#---------------------------------------------------------------------------------------------------------
'''
JOIN - Joins with another DataFrame
join(DataFrame, <list of col names | single col | a join expression>,
                <'inner|cross|left_outer|right_outer|outer|left_semi|right_semi|left_anti'>)
'''
#---------------------------------------------------------------------------------------------------------
# emp_data_file = '/Users/............/........../TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/emp_data_ORIG.csv'
# dept_data_file = '/Users/........../............/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/dept_data.csv'
# empDf = ss.read.format('csv').option('header','true').load(emp_data_file)
# deptDf = ss.read.format('csv').option('header','true').load(dept_data_file)

# joined_data = empDf.join(deptDf, empDf.deptid == deptDf.dept_id, 'inner').show()
# emp = empDf.alias('emp')
# dept = empDf.alias('dept')
# joined_data = emp.join(dept, emp.dept_id == dept.dept_id, 'inner')
# joined_data2 = joined_data.select(emp.dept_id,'first_name','email','dept_name').show()

## 1. Note:  Remove duplicate columns using DROP >>>
# joined_data = empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, 'inner').drop(deptDf.dept_id).drop(deptDf.caption).show()

## 2. Note:  Will remove duplicate columns automatically >>>
# joined_data = empDf.join(deptDf, ['dept_id'] , 'inner').show()

## 3. Note: What if column names are different and you want to use the above expression >>>
##          -> RENAME the column before performing join >>>
# emp = empDf.alias('emp').withColumnRenamed('deptid','dept_id')
# joined_data = emp.join(deptDf, ['dept_id'] , 'inner')
# joined_data.show()

## 4. Note: join using multiple columns >>>
# inspection = '/Users/........./............./TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/inspections_plus.csv'
# violations = '/Users/............../................./TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/violations_plus.csv'
#
# from pyspark.sql.types import StructField, StructType
# from pyspark.sql.types import StringType, IntegerType
#
# inspection_schema = StructType(
#     [
#         StructField('location_id',IntegerType(),True),
#         StructField('inspection_id',IntegerType(),True),
#         StructField('inspection_date',StringType(),True),
#         StructField('description',StringType(),True),
#     ]
# )
#
# violations_schema = StructType(
#     [
#         StructField('location_id',IntegerType(),True),
#         StructField('violation_date',StringType(),True),
#         StructField('violation_code', IntegerType(),True),
#         StructField('violation_category', StringType(),True),
#         StructField('violation_desc',StringType(), True)
#     ]
# )
#
# inspectionDf = ss.read.format('csv').schema(inspection_schema).load(inspection)
# violationsDf = ss.read.format('csv').schema(violations_schema).load(violations)
# vDf = violationsDf.withColumnRenamed('violation_date','date')
# iDf = inspectionDf.withColumnRenamed('inspection_date','date')

# joined_data = inspectionDf.join(violationsDf, (inspectionDf.location_id == violationsDf.location_id) & \
#                                               (inspectionDf.inspection_date == violationsDf.violation_date),
#                                 'inner').drop(violationsDf.violation_date).drop(violationsDf.location_id).show()

# joined_data = iDf.join(vDf, ['location_id','date'], 'inner')
# joined_data.show()


#---------------------------------------------------------------------------------------------------------
'''
How Spark performs JOIN operations. Use EXPLAIN. >>>
Join Types:
  - Shuffle Hash Join
  - Broadcast Hash Join
  - Sort Merge Join
  
How did we go groupBy using Pair RDD??
Step 1: Created a KV RDD using keyBy transformation.
Step 2: Used groupByKey to group per key that was created in Step 1.
What about reduceByKey?
How to see EXPLAIN PLAN
'''
#---------------------------------------------------------------------------------------------------------
# from pyspark.sql.functions import broadcast
# joined_data = inspectionDf.join(broadcast(violationsDf), (inspectionDf.location_id == violationsDf.location_id) & \
#                                               (inspectionDf.inspection_date == violationsDf.violation_date),
#                                 'inner').drop(violationsDf.violation_date).drop(violationsDf.location_id)

# joined_data.show()
# joined_data.explain()     ## To check only the physical plan
# joined_data.explain(True) ## To get all plans


# LIT function
#------------------
# from pyspark.sql.functions import lit
#
# nu = 10
# chr = "Shommodeep Dey"
# dec = 24.08
#
# df = violationsDf.select(lit(nu),lit(chr),lit(dec))
# df.printSchema()
# print(df.dtypes)


#---------------------------------------------------------------------------------------------------------
# groupBy and agg - Pass the no. of columns and get aggregate value from it
# withColumn, avg, count, max, mean, min, sum, countDistinct, sumDistinct
#---------------------------------------------------------------------------------------------------------
from pyspark.sql.functions import max, min,sum,avg, col, countDistinct, count
from pyspark.sql.types import DecimalType, IntegerType
# bank_data = '/Users/......../.............../TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/JPMC_Bank_Database.csv'
# bankDf = ss.read.format('csv').option('header','true').load(bank_data)
# bankDf.printSchema()

# df1 = bankDf.select(count('Institution_Name')).show()
# df1 = bankDf.select(countDistinct('Institution_Name')).show()

# bankDf.printSchema()
# df1 = bankDf.withColumn('diff_in_deposit_16_15', col('2016_Deposits').cast(IntegerType()) - col('2015_Deposits').cast(IntegerType())).show()
# df1 = bankDf.withColumn('diff_in_deposit_16_15', col('2016_Deposits').cast(IntegerType()) - col('2015_Deposits').cast(IntegerType())).withColumn('diff_15_14',col('2015_Deposits').cast(IntegerType()) - col('2014_Deposits').cast(IntegerType())).show()
# df1 = bankDf.select(max("2013_Deposits"), min("2016_Deposits"))
# df1.show()

## 1. Note: Change the column name after aggregation >>>
## 2. Note: Change Data Type of resulting columns >>>
# df1 = bankDf.select(min("2014_Deposits"), avg("2015_Deposits")).withColumnRenamed('avg(2015_Deposits)','avg_2015_deposits').withColumnRenamed('sum(2014_Deposits)','sum_of_2014_deposits').show()
# df1 = bankDf.select(sum("2014_Deposits").alias('sum_2014'), avg("2015_Deposits").alias('avg_2015')).show()
# df2 = df1.select(col('sum_2014').cast(IntegerType()), col('avg_2015').cast(DecimalType(12,2)))
# df2.show()


# SELECT AVG(2014_Deposits), MAX(2015_Deposits), MIN('2016_Deposits') FROM BANK GROUP BY city, state
# df1 = bankDf.groupBy("city","state").agg({'2014_Deposits': 'avg', '2015_Deposits': 'max', '2016_Deposits': 'min'}).explain()
# df1 = bankDf.groupBy("city","state").agg(avg('2014_Deposits').alias('avg_2014_dep'), max('2015_Deposits'), min('2016_Deposits')).show()



## Practice Questions: Solve the business problems mentioned
# bankfile = '/Users/............../.............../TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/JPMC_Bank_Database.csv'
# bankDf = ss.read.format('csv').option('header','true').load(bankfile)

#?? QNS 1 >>> Find the oldest banks among the lot
#---------------------------------------------------------------------------
from pyspark.sql.types import DateType
from pyspark.sql.functions import lit, to_date, to_timestamp, min

# Option 1 :-
# bankDf1 = bankDf.select('Main_Office','Branch_Name','Branch_Number',to_date('Established_Date',"MM/dd/yyyy").alias('Established_Date'))
# bankDf2 = bankDf1.groupBy('Main_Office','Branch_Name','Branch_Number').agg(min('Established_Date').alias('Established_Date'))
# bankDf3 = bankDf2.orderBy(col('Established_Date').asc()).show()


# Option 2 :-
# bankDf1 = bankDf.select('Main_Office','Branch_Name','Branch_Number',to_date('Established_Date',"mm/dd/yyyy").alias('Established_Date'))
# minDate = bankDf1.select(min('Established_Date').alias('Established_Date'))
# for i in minDate.collect()[0]:
#     min_date = i
#     print(min_date)
#
# bankDf2 = bankDf1.filter(col('Established_Date') == min_date)
# bankDf2.show(150,False)


#?? QNS 2 >>> Find total deposit of 2016 and 2015 by County and State
#---------------------------------------------------------------------------
# print(bankDf.select('County','State').distinct().count())
# df1 = bankDf.select('Main_Office','County','State','2015_Deposits','2016_Deposits')
# df2 = df1.groupBy('County','State','Main_Office').agg(sum('2015_Deposits').cast(DecimalType(10,2)).alias('tot_2015_deposits'), sum('2016_Deposits').cast(DecimalType(10,2)).alias('tot_2016_deposits'))
# df3 = df2.orderBy(df2.tot_2016_deposits.desc())
# df3.show(200,False)


#?? QNS 3 >>> Find the individual zip codes and total no. of branches per zip code
#---------------------------------------------------------------------------------
# print(bankDf.select('Zipcode').distinct().count())
# bankDf.groupBy('Zipcode').agg(count('Branch_Name').alias('total_branches')).show()


# using car sales dataset
# car_sales_file = '/Users/........../.............../TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_information.json'
# carDf = ss.read.format('json').option('inferSchema','true').load(car_sales_file)
# carDf.printSchema()

#?? QNS 1 >>> Which product was sold the most by Quantity - find top 5
# --------------------------------------------------------------------
# df1 = carDf.select('product_name','quantity_sold').groupBy('product_name').agg(sum('quantity_sold').alias('tot_quantity_sold'))
# df2 = df1.sort(df1.tot_quantity_sold.desc())
# df2.limit(5).show()


#?? QNS 2 >>> Who are the product manufacturers
# ---------------------------------------------
# carDf.selectExpr('product_make').distinct().show()


#?? QNS 3 >>> Which model was sold in which country the most - top 25
# -------------------------------------------------------------------
from pyspark.sql.functions import desc
# df1 = carDf.select('product_name','country_sold_in')
# df2 = df1.groupBy('product_name','country_sold_in').agg(count('product_name').alias('tot_product_sold'))

# using ORDER BY (orderBy)
# df3 = df2.orderBy(df2.tot_product_sold.desc()).limit(25)
# df3 = df2.orderBy(desc("tot_product_sold")).limit(25)
# df3 = df2.orderBy(["tot_product_sold"], ascending=[0]).limit(25)

# Using SORT
# df3 = df2.sort("tot_product_sold", ascending=False).limit(5)
# df3 = df2.sort(desc("tot_product_sold")).limit(5)
# df3 = df2.sort(df2.tot_product_sold.desc()).limit(5)

# df3.show()


#?? QNS 4 >>> Statewise sale figure in each country except USA
# -------------------------------------------------------------
# df1 = carDf.select('country_sold_in','state_sold_in','quantity_sold')
# df2 = df1.groupBy('country_sold_in','state_sold_in').agg(sum('quantity_sold').alias('tot_car_sold'))
# df3 = df2.where("country_sold_in != 'United States'")
# df3 = df2.where(col('country_sold_in') != 'United States')
# df3 = df2.where(col('country_sold_in').__ne__('United States'))
# df3.show()


#?? QNS 5 >>> Details of Car make, Product Name and Total Quantity of the oldest car
#-----------------------------------------------------------------------------------
# oldestDate = carDf.select(min('model_year'))
# for i in oldestDate.collect()[0]:
#     print(i)
#
# df1 = carDf.select('product_make','product_name','quantity_sold','model_year')
# df2 = df1.filter(col('model_year') == i)
# df2.show()
#
# carDf.where(col('model_year') == 1909).show()


#-----------------------------------------------------------------------------------------------
'''
Partitioning in DataFrames:
- repartition (<no. of partitions>, <list of columns>)  Uses Hash Partitioner
- repartitionByRange(<no. of partitions>, <list of range columns>) Uses Range Partitioner 
- Configuration 'spark.default.parallelism'
Note: Can be mentioned while writing DataFrames to files too.
'''
#-----------------------------------------------------------------------------------------------
# car_file = '/Users/........./.........../TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_data.json'
# df = ss.read.format('json').option('inferSchema','true').load(car_file)
# print(df.rdd.getNumPartitions())

# df1 = df.select('product_name','quantity_sold','country_sold_in','state_sold_in').repartition(4)
# df1 = df.select('product_name','quantity_sold','country_sold_in','state_sold_in').repartition(4,['country_sold_in','state_sold_in'])
# print(df1.rdd.getNumPartitions())

# df1 = df.select('product_name','quantity_sold','country_sold_in','state_sold_in').repartitionByRange(4,['country_sold_in','state_sold_in'])
# print(df1.rdd.getNumPartitions())

# for i in df1.rdd.glom().collect():
#     print(len(i))

#---------------------------------------------------------------------------------------------
# Window functions
#---------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# simpleData = (("James", "Sales", 3000), \
#               ("Michael", "Sales", 4600), \
#               ("Robert", "Sales", 4100), \
#               ("Maria", "Finance", 3000), \
#               ("James", "Sales", 3000), \
#               ("Scott", "Finance", 3300), \
#               ("Jen", "Finance", 3900), \
#               ("Jeff", "Marketing", 3000), \
#               ("Kumar", "Marketing", 2000), \
#               ("Saif", "Sales", 4100) \
#               )
#
# columns = ["employee_name", "department", "salary"]
#
# df = spark.createDataFrame(data=simpleData, schema=columns)
#
# # df.printSchema()
# # df.show(truncate=False)
#
# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number
#
# windowSpec = Window.partitionBy("department").orderBy("salary")
#
# df.withColumn("row_number", row_number().over(windowSpec)) \
#     .show(truncate=False)

# from pyspark.sql.functions import rank
#
# df.withColumn("rank", rank().over(windowSpec)) \
#     .show()
#
# from pyspark.sql.functions import dense_rank
#
# df.withColumn("dense_rank", dense_rank().over(windowSpec)) \
#     .show()
#
# from pyspark.sql.functions import percent_rank
#
# df.withColumn("percent_rank", percent_rank().over(windowSpec)) \
#     .show()
#
# from pyspark.sql.functions import ntile
#
# df.withColumn("ntile", ntile(2).over(windowSpec)) \
#     .show()
#
# from pyspark.sql.functions import cume_dist
#
# df.withColumn("cume_dist", cume_dist().over(windowSpec)) \
#     .show()
#
# from pyspark.sql.functions import lag
#
# df.withColumn("lag", lag("salary", 2).over(windowSpec)) \
#     .show()
#
# from pyspark.sql.functions import lead
#
# df.withColumn("lead", lead("salary", 2).over(windowSpec)) \
#     .show()
#
# windowSpecAgg = Window.partitionBy("department")
# from pyspark.sql.functions import col, avg, sum, min, max, row_number
#
# df.withColumn("row", row_number().over(windowSpec)) \
#     .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
#     .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
#     .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
#     .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
#     .where(col("row") == 1).select("department", "avg", "sum", "min", "max") \
#     .show()


from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import arrays_zip, col, explode
from pyspark.sql.types import *

df = spark.createDataFrame( [('Bob', 16, ['Maths','Physics','Chemistry'], ['A','B','C'])], \
    ['Name','Age','Subjects', 'Grades'])
df2 = spark.createDataFrame( [('Bob', 16, ['Maths','Physics','Chemistry'], {'addr1':'A','addr2':'B','addr3':'C'})], \
    ['Name','Age','Subjects', 'Grades'])

# df = df.withColumn("new", arrays_zip("Subjects", "Grades"))
# df.show(truncate=False)
# df = df.withColumn("new", arrays_zip("Subjects", "Grades")).withColumn("new", explode("new"))
# df.show(truncate=False)
# df = df.withColumn("new", arrays_zip("Subjects", "Grades")).withColumn("new", explode("new")) \
# .select("Name", "Age", col("new.Subjects"), col("new.Grades"))
# df = df.withColumn("subject", explode("Subjects")).select("Name","Age","Grades","subject").withColumn("grade",explode("Grades"))
df2 = df2.select(df2.Name,df2.Subjects,explode(df2.Grades)).withColumn("Grades",explode(df2.Subjects))
df2.show(truncate=False)
