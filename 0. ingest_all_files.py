# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("1. ingest_crkts_file" , 0, {'p_Data_source': 'Ergast API'})

# COMMAND ----------

v_result

# COMMAND ----------

