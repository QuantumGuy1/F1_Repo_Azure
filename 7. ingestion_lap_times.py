# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.formula1dlmyproject.dfs.core.windows.net" , 
    "<ACCESSKEY>"
)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

# COMMAND ----------

lap_times_Schema = StructType(fields=[ #this is a row # these are the columns
    StructField("raceId", IntegerType(), False), 
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)

])

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_Schema) \
        .csv("abfss://raw@formula1dlmyproject.dfs.core.windows.net/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed('raceId' , 'race_id') \
    .withColumnRenamed('driverId' , 'driver_id') \
        .withColumn('ingestion_date' , current_timestamp())

# COMMAND ----------

final_df.write.mode('overwrite').parquet("abfss://processed@formula1dlmyproject.dfs.core.windows.net/LAP_TIMES")

# COMMAND ----------

display(spark.read.parquet('abfss://processed@formula1dlmyproject.dfs.core.windows.net/LAP_TIMES'))