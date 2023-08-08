# Databricks notebook source
# MAGIC %md
# MAGIC # Padding arround string

# COMMAND ----------

l = [('x',)]

# COMMAND ----------

df = spark.createDataFrame(l).toDF("dummy")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import lit,lpad

# COMMAND ----------

df.select(lpad(lit("Hello"),10,"_").alias("dummy")).show()

# COMMAND ----------

df.select(rpad(lpad(lit("Hello"),10,"_"),15,"-").alias("dummy")).show()


# COMMAND ----------

df.select(rpad(lpad(lit("Hello"),10,"_"),11,"-").alias("dummy")).show()


# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 
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

employeesDF = spark.createDataFrame(employees).toDF("employee_id", "first_name","last_name","salary","nationality","phone_number","ssn")

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employeesDF.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lpad,rpad,concat

# COMMAND ----------

empFixedDF = employeesDF.select(
    concat(
        rpad(lpad("employee_id",5,"0"),6,"-"),
        rpad("first_name",10,"-"),
        rpad("last_name",10,"-"),
        lpad("salary",10,"0"),
        rpad("nationality",15,"-"),
        rpad("phone_number",20,"-"),
        "ssn"
    ).alias("employee")
)

# COMMAND ----------

empFixedDF.show(truncate=False)

# COMMAND ----------

