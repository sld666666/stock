
SPARK_HOME = '/Users/luodongshen/Documents/soft/spark-3.0.0-bin-hadoop3.2'
import findspark
findspark.add_packages('org.mongodb.spark:mongo-spark-connector_2.11:2.3.1')
findspark.init(SPARK_HOME)

from pyspark.sql import SparkSession

logFile = "/Users/luodongshen/Documents/stock_logs/stock_info.log"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
