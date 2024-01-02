# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.formula1dlmyproject.dfs.core.windows.net" , 
    "<ACCESSKEY>"
)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

# COMMAND ----------

quali_Schema = StructType(fields=[ #this is a row # these are the columns
    StructField("qualifyId", IntegerType(), False), 
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

quali_df = spark.read \
    .schema(quali_Schema) \
        .option('multiline', True) \
            .json("abfss://raw@formula1dlmyproject.dfs.core.windows.net/qualifying")

# COMMAND ----------

display(quali_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = quali_df.withColumnRenamed('raceId' , 'race_id') \
    .withColumnRenamed('driverId' , 'driver_id') \
        .withColumnRenamed('qualifyId' , 'qualify_id') \
            .withColumnRenamed('constructorId' , 'constructor_id') \
                .withColumn('ingestion_date' , current_timestamp())

# COMMAND ----------

final_df.write.mode('overwrite').parquet("abfss://processed@formula1dlmyproject.dfs.core.windows.net/QUALIFYING")

# COMMAND ----------

display(spark.read.parquet('abfss://processed@formula1dlmyproject.dfs.core.windows.net/QUALIFYING'))

# COMMAND ----------

