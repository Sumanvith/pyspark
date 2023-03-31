from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local[*]").setAppName("first")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

df = spark.read.format("csv").option("header","true").load("file:///C:/data/txns_head.csv")
df.show()

df.createOrReplaceTempView("tdf")
procdf = spark.sql("select * from tdf where category='Gymnatics'")
procdf.show()