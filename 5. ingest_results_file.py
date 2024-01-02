# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Import spark config

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlmyproject.dfs.core.windows.net" , 
    "<ACCESSKEY>"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 1: read json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

# COMMAND ----------

results_Schema = StructType(fields=[ #this is a row
    StructField("resultId", IntegerType(), False), # these are the columns
    StructField("raceId", IntegerType(), True), 
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", StringType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", StringType(), True)

])

# COMMAND ----------

results_df = spark.read \
    .schema(results_Schema) \
        .json("abfss://raw@formula1dlmyproject.dfs.core.windows.net/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2: rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", 'result_id') \
    .withColumnRenamed('raceId' , 'race_id') \
        .withColumnRenamed('driverId' , 'driver_id') \
            .withColumnRenamed('constructorId' , 'constructor_id') \
                .withColumnRenamed('positionText' , 'position_text') \
                    .withColumnRenamed('positionOrder' , 'position_order') \
                        .withColumnRenamed('fastestLap' , 'fastest_lap') \
                            .withColumnRenamed('fastestLapTime' , 'fastest_lap_time') \
                                .withColumnRenamed('fastestLapSpeed' , 'fastest_lap_speed') \
                                    .withColumn('ingestion_date' , current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### step 3: drop unwanted columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col('StatusId'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: write the output to the processed containers

# COMMAND ----------

results_final_df.write.mode('overwrite').parquet("abfss://processed@formula1dlmyproject.dfs.core.windows.net/RESULTS")

# COMMAND ----------

display(spark.read.parquet('abfss://processed@formula1dlmyproject.dfs.core.windows.net/RESULTS'))

# COMMAND ----------

