from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("ReadTextFile").getOrCreate()

# Read the text file into a DataFrame
df = spark.read.text("C:\\data\\dt.txt")

# Show the contents of the DataFrame
df.show()

# Stop the SparkSession
spark.stop()
