# Databricks notebook source
l = [('    Hello!    ',)]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("dummy")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import col, ltrim, rtrim, trim

# COMMAND ----------

df.withColumn("ltrim", ltrim("dummy")).show()

# COMMAND ----------

df.withColumn("ltrim", ltrim("dummy")).\
    withColumn("rtrim", rtrim(col("dummy"))).show()


# COMMAND ----------

df.withColumn("ltrim", ltrim("dummy")).\
    withColumn("rtrim", rtrim(col("dummy"))).\
    withColumn("trim", trim("dummy")).show()


# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION rtrim').show(truncate=False)

# COMMAND ----------

# if we don't specify the trimStr, its default to space
df.withColumn("ltrim", ltrim("dummy")).\
    withColumn("rtrim", rtrim(col("dummy"))).\
    withColumn("trim", expr("trim('!',trim(dummy))")).show()

# COMMAND ----------

# if we don't specify the trimStr, its default to space
df.withColumn("ltrim", ltrim("dummy")).\
    withColumn("rtrim", rtrim(col("dummy"))).\
    withColumn("trim", expr("trim('!',rtrim(dummy))")).show()

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION trim').show(truncate=False)

# COMMAND ----------

df.withColumn("ltrim", expr("trim(LEADING ' ' FROM dummy)")).show()

# COMMAND ----------

df.withColumn("ltrim", expr("trim(LEADING ' ' FROM dummy)"))\
    .withColumn("Rtrim", expr("trim(TRAILING ' ' FROM dummy)")).show()


# COMMAND ----------

df.withColumn("ltrim", expr("trim(LEADING ' ' FROM dummy)"))\
    .withColumn("Rtrim", expr("trim(TRAILING ' ' FROM dummy)"))\
    .withColumn("trim", expr("trim(BOTH ' ' FROM dummy)")).show()



# COMMAND ----------

df.withColumn("ltrim", expr("trim(LEADING ' ' FROM dummy)"))\
    .withColumn("Rtrim", expr("trim(TRAILING ' ' FROM dummy)"))\
    .withColumn("trim", expr("trim(BOTH ' ' '!' FROM dummy)")).show()



# COMMAND ----------

