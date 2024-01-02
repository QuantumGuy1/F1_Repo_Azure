# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### produce driver standings

# COMMAND ----------

# MAGIC %run "../INCLUDES/CONFIGURATION"

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlmyproject.dfs.core.windows.net" , 
    "<ACCESSKEY>"
)

# COMMAND ----------

race_res_df = spark.read.parquet(f'{presentation_folder_path}/RACE_RESULTS')

# COMMAND ----------

display(race_res_df)

# COMMAND ----------

from pyspark.sql import functions as F

team_Standings_df = race_res_df.groupBy("race_year", "team").agg(
    F.sum("points").alias("total_points"), 
    F.count(F.when(F.col("position") == 1, True)).alias("wins")
)

# COMMAND ----------

print(type(team_Standings_df))

# COMMAND ----------

display(team_Standings_df.where('race_year = 2020'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

team_rank_Spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))

final_df= team_Standings_df.withColumn('rank', rank().over(team_rank_Spec))

# COMMAND ----------

display(final_df.where('race_year = 2020'))

# COMMAND ----------

final_df.write.mode('Overwrite').format('parquet').saveAsTable('f1_presentation.constructors_standings')

# COMMAND ----------

