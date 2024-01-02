# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest races file

# COMMAND ----------

# MAGIC %md 
# MAGIC ## STEP 1: SETUP SPARK CONFIG

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlmyproject.dfs.core.windows.net" , 
    "<ACCESSKEY>"
)


# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formula1dlmyproject.dfs.core.windows.net/races1.csv"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[ #this is a row
    StructField("raceId", IntegerType(), False), # these are the columns
    StructField("year", IntegerType(), True), 
    StructField("round", IntegerType(), True),
    StructField("circuitid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
        .schema(races_schema) \
            .csv("abfss://raw@formula1dlmyproject.dfs.core.windows.net/races1.csv") #performs 2 spark jobs which makes it slow

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## step 2: add ingestion date and race_timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , col, lit, to_timestamp, concat

# COMMAND ----------

races_with_timestamp = races_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp" , to_timestamp(concat(col('date') , lit(' ') , col('time')) , 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## step3: select only columns and rename them as required

# COMMAND ----------

races_Select_df = races_with_timestamp.select(col('raceId').alias('race_id') , col('year').alias("race_year"), col('round'), col('circuitid').alias("circuit_id"), col('name').alias('race_name'), col('ingestion_date') , col("race_timestamp"))

# COMMAND ----------

display(races_Select_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## write data to processed container in parquet

# COMMAND ----------

races_Select_df.write.mode('overwrite').partitionBy('race_year').parquet('abfss://processed@formula1dlmyproject.dfs.core.windows.net/RACES')

# COMMAND ----------

display(spark.read.parquet('abfss://processed@formula1dlmyproject.dfs.core.windows.net/RACES'))

# COMMAND ----------

display(dbutils.fs.ls('abfss://processed@formula1dlmyproject.dfs.core.windows.net/RACES'))

# COMMAND ----------

