from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local[*]").setAppName("first")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

avrodf = spark.read.format("avro").load("file:///C:/data/part.avro")
avrodf.show()