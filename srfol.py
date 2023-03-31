from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local[*]").setAppName("first").set("spark.driver.allowMultipleContexts", "true")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import *

df = spark.read.format("csv").option("header", "true").load("file:///C:/data/usdata.csv")
df.show()
df.createOrReplaceTempView("cdf")

procdf = spark.sql("select * from cdf where state = 'LA'")

procdf.show()