# Databricks notebook source
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

employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )

# COMMAND ----------

employeesDF.select('first_name','last_name').show()

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employeesDF.groupBy('nationality').count().show()

# COMMAND ----------

employeesDF.orderBy("employee_id").show()


# COMMAND ----------

from pyspark.sql.functions import col,upper

# COMMAND ----------

employeesDF.select(upper('first_name'),upper('last_name')).show()

# COMMAND ----------

employeesDF.groupBy(upper('nationality')).count().show()

# COMMAND ----------

# This will fail as the function desc is available only on column type.
employeesDF. \
    orderBy("employee_id".desc()). \
    show()

# COMMAND ----------

# This will fail as the function desc is available only on column type.
employeesDF. \
    orderBy(col("employee_id").desc()). \
    show()

# COMMAND ----------

employeesDF.orderBy(col("first_name").desc()).show()

# COMMAND ----------

employeesDF.orderBy(employeesDF["first_name"].desc()).show()


# COMMAND ----------

employeesDF.orderBy(upper(employeesDF.first_name).desc()).show()


# COMMAND ----------

from pyspark.sql.functions import lit,concat,col

# COMMAND ----------

employeesDF.select(concat('first_name',lit(', '),'last_name').alias('full_name')).show()

# COMMAND ----------

employeesDF.select(concat(employeesDF.first_name,lit(', '),col('last_name')).alias('full_name')).show()


# COMMAND ----------

