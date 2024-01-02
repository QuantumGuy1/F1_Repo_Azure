# Databricks notebook source
# MAGIC %md
# MAGIC ## READ AND PROCESS MULTI LINE JSON FILE

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 0: setup spark config

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

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

# COMMAND ----------

pit_stops_Schema = StructType(fields=[ #this is a row # these are the columns
    StructField("raceId", IntegerType(), False), 
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_Schema) \
        .option('multiline' , True) \
            .json("abfss://raw@formula1dlmyproject.dfs.core.windows.net/pitstops.json")

# COMMAND ----------

display(pit_stops_df) # shows null because spark cannot handle by default multi line values to avoid it add 'multiline as true

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed('driverId' , 'driver_id') \
    .withColumnRenamed('raceId' , 'race_id') \
        .withColumn('ingestion_date' , current_timestamp())           

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: write to parquet file in processed container

# COMMAND ----------

pit_stops_final_df.write.mode('overwrite').parquet("abfss://processed@formula1dlmyproject.dfs.core.windows.net/PIT_STOPS")

# COMMAND ----------

display(spark.read.parquet('abfss://processed@formula1dlmyproject.dfs.core.windows.net/PIT_STOPS'))

# COMMAND ----------

