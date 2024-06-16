# Databricks notebook source
l = [('x',)]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("dummy")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import current_date,current_timestamp

# COMMAND ----------

df.select(current_date()).show()

# COMMAND ----------

df.select(current_timestamp()).show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import lit, to_date, to_timestamp

# COMMAND ----------

df.select(to_date(lit('20210228'),'yyyyMMdd').alias('to_date')).show()

# COMMAND ----------

df.select(to_timestamp(lit('20190215 2323'),'yyyyMMdd HHmm').alias("time_stamp")).show()

# COMMAND ----------

df.select(to_timestamp(lit('20190215 '),'yyyyMMdd ').alias("time_stamp")).show()


# COMMAND ----------

