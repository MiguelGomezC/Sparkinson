import os
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_251\\'
from pyspark import SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json("Bicimad_Estacions_201807.json")
df.show()

"""
sc = SparkContext()
rdd = sc.textFile('sample_10e3.json')

"""