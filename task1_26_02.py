from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local[*]").setAppName("first")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header", "true").csv("file:///C:/data/df.csv")
df1 = spark.read.option("header", "true").csv("file:///C:/data/df1.csv")
df.show()
df1.show()
df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
spark.sql("select * from df order by id").show()
spark.sql("select * from df1  order by id").show()

print("Select two columns")
spark.sql("select id,tdate from df  order by id").show()

print("Select column with category filter = Exercise")
spark.sql("select id,tdate,category from df where category='Exercise' order by id").show()

print("Multi Column filter")
spark.sql("select id,tdate,category,spendby from df where category='Exercise' and spendby='cash' ").show()

print("Multi Value Filter")
spark.sql("select * from df where category in ('Exercise','Gymnastics')").show()
