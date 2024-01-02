# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### produce driver standings

# COMMAND ----------

# MAGIC %run "../INCLUDES/CONFIGURATION"

# COMMAND ----------



# COMMAND ----------

race_res_df = spark.read.parquet(f'{presentation_folder_path}/RACE_RESULTS')

# COMMAND ----------

display(race_res_df)

# COMMAND ----------

print(type(race_res_df))

# COMMAND ----------

from pyspark.sql import functions as F

driver_Standings_df = race_res_df.groupBy("race_year", "driver_name", "driver_nationality", "team").agg(
    F.sum("points").alias("total_points"), 
    F.count(F.when(F.col("position") == 1, True)).alias("wins")
)

# COMMAND ----------

print(type(driver_Standings_df))

# COMMAND ----------

display(driver_Standings_df.where('race_year = 2020'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_Spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))

final_df= driver_Standings_df.withColumn('rank', rank().over(driver_rank_Spec))

# COMMAND ----------

display(final_df.where('race_year = 2020'))

# COMMAND ----------

final_df.write.mode('Overwrite').format('parquet').saveAsTable('f1_presentation.driver_standings')

# COMMAND ----------

