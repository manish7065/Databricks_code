# Databricks notebook source
from pyspark.sql import functions

# COMMAND ----------

datetimes = [(20140228, "28-Feb-2014 10:00:00.123"),
                     (20160229, "20-Feb-2016 08:08:08.999"),
                     (20171031, "31-Dec-2017 11:59:59.123"),
                     (20191130, "31-Aug-2019 00:00:00.000")
                ]

# COMMAND ----------

datetimesDF = spark.createDataFrame(datetimes,schema="date BIGINT, time STRING")

# COMMAND ----------

datetimesDF.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import lit,to_date

# COMMAND ----------

l = [('x',)]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("dummy")

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(to_date(lit('20210302'),'yyyyMMdd').alias('to_date')).show()

# COMMAND ----------

# year and day of year to standard date
df.select(to_date(lit('2021061'),'yyyyDDD').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02/03/2021'),'dd/MM/yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-03-2021'),'dd-MM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_date(lit('02-march-2021'),'dd-MMMM-yyyy').alias('to_date')).show()


# COMMAND ----------

df.select(to_date(lit('march 2, 2021'),'MMMM d, yyyy').alias('to_date')).show()


# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# COMMAND ----------

df.select(to_timestamp(lit('02-mar-2012'),'dd-MMM-yyyy').alias('to_date')).show()

# COMMAND ----------

df.select(to_timestamp(lit('02-mar-2021 17:30:15'), 'dd-MMM-yyyy HH:mm:ss').alias('to_date')).show()

# COMMAND ----------

datetimesDF.printSchema()

# COMMAND ----------

datetimesDF.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, to_timestamp

# COMMAND ----------

datetimesDF\
.withColumn('to_date', to_date(col('date').cast('string'),'yyyyMMdd'))\
.withColumn('to_timestamp', to_timestamp(col('time').cast('string'),'dd-MMM-yyyy HH:mm:ss.SSS')).show(truncate=False)


# COMMAND ----------

