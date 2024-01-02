# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingest Drivers json file
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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema =  StructType(fields=[ 
    StructField("forename", StringType(), True), 
    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[ #this is a row
    StructField("driverId", IntegerType(), False), # these are the columns
    StructField("driverRef", StringType(), True), 
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema), 
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
        .json("abfss://raw@formula1dlmyproject.dfs.core.windows.net/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed('driverId' , 'driver_id') \
    .withColumnRenamed('driverRef' , 'driver_ref') \
        .withColumn('ingestion_date', current_timestamp()) \
            .withColumn('name' , concat(col('name.forename') , lit(' ') , col('name.surname')))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: drop unwanted columns

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### step 4: write the output to processed container in parquet

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet("abfss://processed@formula1dlmyproject.dfs.core.windows.net/DRIVERS")

# COMMAND ----------

