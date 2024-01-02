# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingest CSV circuits file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('p_Data_source' , '')
v_data_source = dbutils.widgets.get('p_Data_source')

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../INCLUDES/CONFIGURATION"

# COMMAND ----------

# MAGIC %run "../INCLUDES/COMMON_FUNCTIONS"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP1 : READ THE CSV FILE USING SPARK DATAFRAME READER

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlmyproject.dfs.core.windows.net" , 
    "<ACCESSKEY>"
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formula1dlmyproject.dfs.core.windows.net/circuits1.csv"))

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[ #this is a row
    StructField("circuitId", IntegerType(), False), # these are the columns
    StructField("circuitRef", StringType(), True), 
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),  
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
        .schema(circuits_schema) \
            .csv(f"{raw_folder_path}/circuits1.csv") #performs 2 spark jobs which makes it slow

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SELECT ONLY REQUIRED COLUMNS
# MAGIC ## USE DATAFRAME.SELECT FROM READER API

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_Df = circuits_df.select(col("circuitId") , col("circuitRef") , col("name") , col("location") , col("country").alias("race_country") , col("lat") , col("lng") , col("alt"))

# COMMAND ----------

display(circuits_selected_Df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### step 3 rename columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_rename_df = circuits_selected_Df.withColumnRenamed("circuitId" , "circuit_id") \
    .withColumnRenamed("circuitRef" , "circuit_ref") \
        .withColumnRenamed("lat" , "latitude") \
            .withColumnRenamed("lng" , "longitude") \
                .withColumnRenamed("alt" , "altitude") \
                    .withColumn('data_source' ,lit(v_data_source))

# COMMAND ----------

display(circuits_rename_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 4 add ingestion date to the dateframe

# COMMAND ----------

circuit_final_df = add_ingestion_date(circuits_rename_df)
# .withColumn("env" , lit("Production"))

# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5-WRITE PROCESSED DATA TO DATA LAKE AS PARQUET
# MAGIC

# COMMAND ----------

circuit_final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/CIRCUITS')

# COMMAND ----------

df = spark.read.parquet("abfss://processed@formula1dlmyproject.dfs.core.windows.net/CIRCUITS")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

