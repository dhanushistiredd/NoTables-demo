from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python").getOrCreate()

df = spark.read.format("csv") \
    .options(header="true", inferSchema="true") \
    .load(r"filepath")



df.write.mode("overwrite").parquet( r"filepath")

df_par = spark.read.parquet( r"filepath")

df_par.printSchema()

df_par.show(5)

