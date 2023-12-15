# Databricks notebook source
# DBTITLE 1,Invocar a otras librerias de SQL
from pyspark.sql import SQLContext

sqlContext = SQLContext(spark.sparkContext)

# COMMAND ----------

# DBTITLE 1,Probar el query en SQLContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df_view_imp= spark.sql("select * from global_temp.view_cosechas_clientes")
df_view_imp.display()

# COMMAND ----------

sqlContext.registerDataFrameAsTable(df_view_imp,"db_test_BO")

# COMMAND ----------

sqlContext.sql("select * from db_test_BO").show()
