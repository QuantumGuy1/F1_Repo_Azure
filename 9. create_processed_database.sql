-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS F1_PROCESSED
LOCATION 'abfss://processed@formula1dlmyproject.dfs.core.windows.net/processed'

-- COMMAND ----------

DESCRIBE DATABASE F1_PROCESSED

-- COMMAND ----------

