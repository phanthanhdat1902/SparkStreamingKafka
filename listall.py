import numpy as np
import os
import matplotlib.pyplot as plt
packages = "org.apache.spark:spark-sql_2.12:3.0.1"
os.environ["PYSPARK_SUBMIT_ARGS"] = (
"--packages {0} pyspark-shell".format(packages)
)
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.sql.functions import *
import pyspark.sql.functions as psf
conf = SparkConf()
conf.setMaster('spark://node-master:7077')
conf.setAppName('RHUST')
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
from pyspark.sql.types import *
from pyspark.sql.functions import sum
schema = StructType([
    StructField('GDCD', StringType(), True),
    StructField('HoaHoc', StringType(), True),
    StructField('Su', StringType(), True),
    StructField('NumCity', StringType(),True),
    StructField('KHTN', StringType(),True),
    StructField('KHXH', StringType(),True),
    StructField('NotKnow', StringType(),True),
    StructField('NgoaiNgu', StringType(),True),
    StructField('Van', StringType(),True),
    StructField('MSSV', StringType(),True),
    StructField('Sinh', StringType(),True),
    StructField('Toan', StringType(),True),
    StructField('Ly', StringType(),True),
    StructField('Dia', StringType(),True),
])
df = (spark.read.format("com.databricks.spark.csv")
.option("header", "true")
#.option("inferSchema","true")
.schema(schema)
.load("hdfs://node-master:9000/data_thpt/*.csv"))
df.createOrReplaceTempView("dfTable")
print("so ban ghi:")
# print(df.count())
# df.show(20, False)

#diem TOAN
# mon="Van"
# diem0 = df.where((col(mon) > 0) & (col(mon) < 1)).select(mon).count()
# ##
# diem1 = df.where((col(mon) >= 1) & (col(mon) < 2)).select(mon).count()
# #######
# diem2 = df.where((col(mon) >= 2) & (col(mon) < 3)).select(mon).count()
# #######
# diem3 = df.where((col(mon) >= 3) & (col(mon) < 4)).select(mon).count()
# #######
# diem4 = df.where((col(mon) >= 4) & (col(mon) < 5)).select(mon).count()
# #######
# diem5 = df.where((col(mon) >= 5) & (col(mon) < 6)).select(mon).count()
# #######
# diem6 = df.where((col(mon) >= 6) & (col(mon) < 7)).select(mon).count()
# #######
# diem7 = df.where((col(mon) >= 7) & (col(mon) < 8)).select(mon).count()
# #######
# diem8 = df.where((col(mon) >= 8) & (col(mon) < 9)).select(mon).count()
# #######
# diem9 = df.where((col(mon) >= 9) & (col(mon) < 10)).select(mon).count()
# ####
# diem10 = df.where(col(mon) == 10).select(mon).count()

########
# diem = np.array([diem0, diem1, diem2, diem3, diem4, diem5, diem6, diem7, diem8, diem9, diem10])
# col=["0-1", "1-2", "2-3", "3-4", "4-5", "5-6", "6-7", "7-8", "8-9", "9-10", "10"]
# plt.bar(col, diem, color = 'blue', width = 0.5, alpha = 0.7)
# plt.title('Pho diem mon Van THPT 2020')
# plt.xlabel('Diem')
# plt.ylabel('So hoc sinh')
# plt.show()
other=df.select().count()
KHTN = df.where((col("Sinh") != -1) & (col("Ly") != -1) & (col("HoaHoc") != -1)).select().count()
KHXH = df.where((col("GDCD") != -1) & (col("Su") != -1) & (col("Dia") != -1)).select().count()
print(other)
print(KHTN)
print(KHXH)
diem = np.array([KHTN, KHXH])
label=["KHTN", "KHXH"]
plt.pie(diem, labels=label, startangle = 90, autopct='%.1f%%')
plt.show()
# Toan = df.where(col("Toan")==10).select().count()
# Van = df.where(col("Van") == 10).select().count()
# diem = np.array([Toan, Van])
# col=["Toan","Van"]
# plt.bar(col, diem, color = 'blue', width = 0.5, alpha = 0.7)
# plt.title('So Sanh Van Toan')
# plt.xlabel('Diem')
# plt.ylabel('So hoc sinh')
# plt.show()


# Toan = df.where(col("Toan")==10).select().count()
# GDCD = df.where(col("GDCD") == 10).select().count()
# HoaHoc = df.where(col("HoaHoc") == 10).select().count()
# NgoaiNgu = df.where(col("NgoaiNgu") == 10).select().count()
# Sinh = df.where(col("Sinh") == 10).select().count()
# Ly = df.where(col("Ly") == 10).select().count()
# Dia = df.where(col("Dia") == 10).select().count()
# Su = df.where(col("Su") == 10).select().count()
# Van = df.where(col("Van") == 10).select().count()
# diem = np.array([Toan, Van, GDCD, HoaHoc, NgoaiNgu, Sinh, Ly, Dia, Su])
# label=["Toan", "Van", "GDCD", "HoaHoc", "NgoaiNgu", "Sinh", "Ly", "Dia", "Su"]
# plt.pie(diem, labels=label, startangle = 90, autopct='%.1f%%')
# plt.show()