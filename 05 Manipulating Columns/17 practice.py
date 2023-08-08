# Databricks notebook source
datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]

# COMMAND ----------

df = spark.createDataFrame(datetimes).toDF("date","time")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

df.withColumn("date_ym", date_format("date","yyyyMM"))\
.withColumn("time_ym", date_format("time","yyyyMM"))\
.show(truncate=False)

# COMMAND ----------

df.withColumn("date_ym", date_format("date","yyyyMM").cast('int'))\
.withColumn("time_ym", date_format("time","yyyyMM").cast('int'))\
.printSchema()

# COMMAND ----------

df.withColumn("date_ym", date_format("date","yyyyMMddHHmmss"))\
.withColumn("time_ym", date_format("time","yyyyMMddHHmmss"))\
.show(truncate=False)

# COMMAND ----------

df.withColumn("date_ym", date_format("date","yyyyMMddHHmmss").cast('long'))\
.withColumn("time_ym", date_format("time","yyyyMMddHHmmss").cast('long'))\
.show(truncate=False)

# COMMAND ----------

df.withColumn("date_ym", date_format("date","yyyyDDD"))\
.withColumn("time_ym", date_format("time","yyyyDDD"))\
.show(truncate=False)

# COMMAND ----------

df.withColumn("date_desc",date_format("date", "MMMM d, yyyy")).show(truncate=False)

# COMMAND ----------

df.withColumn("day_name_abbr", date_format("date","EE")).show(truncate=False)

# COMMAND ----------

df.withColumn("day_name_abbr", date_format("date","EEEE")).show(truncate=False)


# COMMAND ----------

