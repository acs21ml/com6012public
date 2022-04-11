
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType


spark = SparkSession.builder \
    .master("local[2]") \
    .appName("COM6012 Spark Intro") \
    .config("spark.local.dir","/fastdata/acs21ml") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")  # This can only affect the log level after it is executed.

# main code
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

logFile=spark.read.text('Data/NASA_access_log_Jul95.gz').cache()

data = logFile.withColumn('host', F.regexp_extract('value', '(.*) - -.*', 1)) \
                .withColumn('timestamp', F.regexp_extract('value', '.* - - \[(.*)\].*', 1)) \
                .withColumn('request', F.regexp_extract('value', '.*\"(.*)\".*', 1)) \
                .withColumn('HTTP reply code', F.split('value', ' ').getItem(F.size(F.split('value', ' '))-2).cast("int")) \
                .withColumn('bytes in the reply', F.split('value', ' ').getItem(F.size(F.split('value', ' '))-1).cast("int")).drop("value").cache()

monthDay = list(map(str, list(range(1, 29))))
weekDay = ['Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']

data2 = data.withColumn('month_day', F.regexp_extract('timestamp', '(.*)/Jul.*', 1).cast('int').cast(StringType()))
data2 = data2.withColumn('week_day', data2.month_day).replace(monthDay, weekDay*4, 'week_day')

results = []
for day in weekDay:
    data_one_weekDay = data2.filter(data2.week_day == day)
    ranked_num_req = data_one_weekDay.groupBy('month_day').count().sort('count').toPandas()
    results.append(ranked_num_req.iloc[-1, 1])
    results.append(ranked_num_req.iloc[0, 1])
    



spark.stop()
