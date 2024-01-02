# Databricks notebook source
# MAGIC %run "../INCLUDES/CONFIGURATION"

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlmyproject.dfs.core.windows.net" , 
    "<ACCESSKEY>"
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## read all data required

# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_folder_path}/DRIVERS') \
    .withColumnRenamed('name', 'driver_name') \
        .withColumnRenamed('number' , 'driver_number') \
            .withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/RACES') \
    .withColumnRenamed('race_timestamp', 'race_date')


circuits_df = spark.read.parquet(f'{processed_folder_path}/CIRCUITS') \
    .withColumnRenamed('name', 'track_name') \
        .withColumnRenamed('location', 'track_venue')


constructors_df = spark.read.parquet(f'{processed_folder_path}/CONSTRUCTORS') \
    .withColumnRenamed('name', 'Team')


results_df = spark.read.parquet(f'{processed_folder_path}/RESULTS') \
    .withColumnRenamed('time', 'race_Time')

# COMMAND ----------

join_df_crkts_races = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id) \
    .select(races_df.race_id, races_df.race_year,races_df.race_date,races_df.race_name, circuits_df.track_venue)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### join result to all other dataframes

# COMMAND ----------

race_results_Df = results_df.join(join_df_crkts_races, results_df.race_id == join_df_crkts_races.race_id) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
        .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_Df.select('race_year' , 'race_date' , 'race_name', 'Track_venue' ,'driver_name', 'driver_nationality' , 'driver_number','team','grid','fastest_lap','race_time','points', 'position') \
    .withColumn('created_date' , current_timestamp())

# COMMAND ----------

display(final_df.where("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc(), final_df.grid.asc()))

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')

# COMMAND ----------

