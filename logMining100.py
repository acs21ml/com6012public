# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("COM6012 Spark Intro") \
    .config("spark.local.dir","/fastdata/acs21ml") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")  # This can only affect the log level after it is executed.

# main code
logFile=spark.read.text("Data/NASA_Aug95_100.txt").cache()
hostsJapan = logFile.filter(logFile.value.contains(".jp")).count()

print("\n\nHello Spark: There are %i hosts from Japan.\n\n" % (hostsJapan))

spark.stop()
