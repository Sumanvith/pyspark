from pyspark import SparkConf
from pyspark import SparkContext

conf = SparkConf().setAppName("MyApp").setMaster("local[*]")

sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")

data = sc.textFile("file:///C:/data/datatxns.txt")

filterdata = data.filter(lambda x: "Vaulting" in x)
res=filterdata.collect()
print("====filterdata data====")

for val in res:
    print(val)

print()
print()

concatdata = filterdata.map(lambda x: (x + ",zeyo"))
res=concatdata.collect()
print("====concatdata data====")
for val in res:
    print(val)
print()
print()
