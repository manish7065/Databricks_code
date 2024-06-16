# Databricks notebook source
l = [('x',)]

# COMMAND ----------

# Oracle dual (view)
# dual - dummy CHAR(1)
# "X" - One record

# COMMAND ----------

df = spark.createDataFrame(l, "dummy STRING")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

l = [('x','y')]
df = spark.createDataFrame(l, "dummy STRING,sec string")
df.printSchema()




# COMMAND ----------

df.show()

# COMMAND ----------

l = [('x',)]
df = spark.createDataFrame(l,"dummy STRING")

from pyspark.sql.functions import current_date
df.select(current_date()).show()

# COMMAND ----------

df.select(current_date()).alias('current_date').show()

# COMMAND ----------

employees = [
    (1, "Scott", "Tiger", 1000.0, 
      "united states", "+1 123 456 7890", "123 45 6789"
    ),
     (2, "Henry", "Ford", 1250.0, 
      "India", "+91 234 567 8901", "456 78 9123"
     ),
     (3, "Nick", "Junior", 750.0, 
      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
     ),
     (4, "Bill", "Gomes", 1500.0, 
      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
     )
]

# COMMAND ----------

len(employees)

# COMMAND ----------

employeesDf = spark.createDataFrame(employees,schema="id INT, first_name STRING,last_name string,salary FLOAT,nationality STRING, phone_number STRING, ssn STRING")

# COMMAND ----------

employeesDf.show()

# COMMAND ----------

employeesDf.printSchema()

# COMMAND ----------

employeesDf.show(truncate=False)

# COMMAND ----------

