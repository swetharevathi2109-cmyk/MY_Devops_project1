from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys  #intract with interpreter
import os   #intract with os
from collections import namedtuple
from pyspark.sql.functions import split


python_path = sys.executable  #py_path is variable  and sys is ensure the correct verison is running
os.environ['PYSPARK_PYTHON'] = python_path  #env variable ensure run pyspark and use python interpreter

# Configure Spark spark context for text file
conf = SparkConf().setAppName("ReadFileRDD").setMaster("local[*]")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.appName("AvroExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.1.2") \
    .getOrCreate() #inport spark sessin for rdd

actdata = namedtuple('actdata',['id','activity','type','payment'])
file_path ='/Users/shweta/Downloads/activities_data.txt'
rdd=sc.textFile(file_path,)
print(rdd.collect())

mapsplit =rdd.map(lambda x: x.split(','))
for row in mapsplit.collect():
    print(row)
rdd = mapsplit.map(lambda x: actdata(x[0],x[1],x[2],x[3]))
colfilter = rdd.filter(lambda x: "gymnastics" in x.type)
for row1 in rdd.collect():
    print(row1)
print(colfilter.collect())

df=colfilter.toDF()
print()
df.show()
df.write.format("avro").save("/Users/shweta/Downloads/avrodata1")
print('done')

