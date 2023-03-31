from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local[*]").setAppName("first")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

csvdf = spark.read.format("csv").option("header", "true").load("file:///C:/data/usdata.csv")

csvdf.show()

csvdf.createOrReplaceTempView("cdf")
procdf = spark.sql(" select * from cdf where state='LA' ")

procdf.show()
procdf.write.format("json").mode("overwrite").save("file:///C:/data/pr")