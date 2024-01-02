# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructors.json file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlmyproject.dfs.core.windows.net" , 
    "<ACCESSKEY>"
)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

constructors_Schema = StructType(fields=[ #this is a row
    StructField("constructorId", IntegerType(), False), # these are the columns
    StructField("constructorRef", StringType(), True), 
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

constructors_df = spark.read \
    .schema(constructors_Schema) \
        .json("abfss://raw@formula1dlmyproject.dfs.core.windows.net/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-2: Drop unwanted columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructors_df.drop(col('url'))



# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df = constructor_dropped_df.withColumnRenamed('constructorId' , 'constructor_id') \
    .withColumnRenamed('constructorRef' , 'constructor_ref') \
        .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 4:write output to parquet file

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet('abfss://processed@formula1dlmyproject.dfs.core.windows.net/CONSTRUCTORS')

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@formula1dlmyproject.dfs.core.windows.net/CONSTRUCTORS"))

# COMMAND ----------

df = spark.read.parquet('abfss://processed@formula1dlmyproject.dfs.core.windows.net/CONSTRUCTORS')

# COMMAND ----------

display(df)

# COMMAND ----------

