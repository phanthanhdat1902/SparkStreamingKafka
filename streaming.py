
import os
from scrapy import Selector
packages = "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages {0} pyspark-shell".format(packages)
)
from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.session import SparkSession
conf = SparkConf()
conf.setMaster('spark://node-master:7077')
conf.setAppName('RHUST')
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
ss=SparkSession(sc)
ssc = StreamingContext(sc, 10)


KAFKA_BROKER = "127.0.0.1:9092"
KAFKA_TOPIC = "RHUST_data"
# Create a schema for the dataframe
schema = StructType([
    StructField('NumCity', StringType(), True),
    StructField('MSSV', StringType(), True),
    StructField('Toan', StringType(), True),
    StructField('Van', StringType(),True),
    StructField('Ly', StringType(),True),
    StructField('HoaHoc', StringType(),True),
    StructField('Sinh', StringType(),True),
    StructField('KHTN', StringType(),True),
    StructField('Su', StringType(),True),
    StructField('Dia', StringType(),True),
    StructField('GDCD', StringType(),True),
    StructField('KHXH', StringType(),True),
    StructField('NgoaiNgu', StringType(),True),
    StructField('NotKnow', StringType(),True),
])

print("RHUST")
kafkaStream = KafkaUtils.createDirectStream(ssc,[KAFKA_TOPIC],{"metadata.broker.list":KAFKA_BROKER})
lines=kafkaStream.map(lambda value:value[1])

def handle_rdd(value):
    result=[]
    City=Selector(text=value).xpath('//td[@class=""]/text()')[1].extract()[:2]
    MSSV=Selector(text=value).xpath('//td[@class=""]/text()')[1].extract()
    # result.insert(0,result[0][:2])
    result.append(City)
    result.append(MSSV)
    sel = Selector(text=value).xpath('//td[@class="mobile-tab-content mobile-tab-2"]')
    for i in sel:
        try:
            result.append(i.xpath("text()")[0].extract())
        except:
            result.append("-1")
    sel = Selector(text=value).xpath('//td[@class="mobile-tab-content mobile-tab-3"]')
    for i in sel:
        try:
            result.append(i.xpath("text()")[0].extract())
        except:
            result.append("-1")
    return result
coords=lines.map(lambda value: handle_rdd(value))
def store_rdd(rdd):
        if not rdd.isEmpty():
                print(type(rdd))
                global ss
                df = ss.createDataFrame(rdd, schema)
                df.write.format('csv').mode('append').option("header", "true").csv("hdfs://node-master:9000/data_thpt")
                df.show()
               # rdd.saveAsTextFile("hdfs://node-master:9000/books")

def empty_rdd():
        print("empty RDD")
coords.foreachRDD(lambda rdd: empty_rdd() if rdd.count() == 0 else store_rdd(rdd))
# coords.pprint()
ssc.start()
ssc.awaitTermination()
