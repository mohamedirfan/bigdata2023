#prerequisites
#spark-submit gcs_spark.py
# Download the jar from https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar

from pyspark.sql.functions import *
from pyspark.sql.types import *
def main():
   from pyspark.sql import SparkSession
   # define spark configuration object
   spark = SparkSession.builder\
      .appName("GCP GCS Read/Write") \
      .enableHiveSupport()\
      .getOrCreate()
   # .config("spark.jars.packages","com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.2.6") \
   #GCS Jar location
   #https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar
   spark.sparkContext.setLogLevel("ERROR")
   sc=spark.sparkContext
   conf = spark.sparkContext._jsc.hadoopConfiguration()
   conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
   conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

   print("Usecase1 Spark Application to Read csv data from cloud GCS and get a DF created with the GCS data in the on prem, "
         "convert csv to json in the on prem DF and store the json into new cloud GCS location")
   print("Hive to GCS to hive starts here")
   gcs_df = spark.read.option("header", "false").option("delimiter", ",").option("inferschema", "true")\
   .csv("gs://com-inceptez-data/custdata/cust12.txt").toDF("id","name","age")
   gcs_df.show()
   print("GCS Read Completed Successfully")
   gcs_df.write.mode("overwrite").partitionBy("age").saveAsTable("default.cust_info_gcs")
   print("GCS to hive table load Completed Successfully")

   print("Hive to GCS usecase starts here")
   gcs_df=spark.read.table("default.cust_info_gcs")
   curts = spark.createDataFrame([1], IntegerType()).withColumn("curts", current_timestamp()).select(date_format(col("curts"), "yyyyMMddHHmmSS")).first()[0]
   print(curts)
   gcs_df.repartition(2).write.json("gs://com-inceptez-data/custdata/cust_json_"+curts)
   print("gcs Write Completed Successfully")

main()
