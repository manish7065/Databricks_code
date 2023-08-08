# Databricks notebook source
s="Hello world!"

# COMMAND ----------

s[:5]

# COMMAND ----------

s[2 : 6]

# COMMAND ----------

l = [('x',)]

# COMMAND ----------

# Creating dummy data frame
df = spark.createDataFrame(l,"dummy STRING")

# COMMAND ----------

from pyspark.sql.functions import substring,lit

# COMMAND ----------

# slicing string using substring
df.select(substring(lit('Hello world'), -5,5)).show()

# COMMAND ----------

df.select(substring(lit('Hello world'), 0,5)).show()


# COMMAND ----------

df.select(substring(lit('Hello world'), 7,5)).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tasks - substring
# MAGIC 
# MAGIC Let us perform few tasks to extract information from fixed length strings.
# MAGIC * Create a list for employees with name, ssn and phone_number.
# MAGIC * SSN Format **3 2 4** - Fixed Length with 11 characters
# MAGIC * Phone Number Format - Country Code is variable and remaining phone number have 10 digits:
# MAGIC  * Country Code - one to 3 digits
# MAGIC  * Area Code - 3 digits
# MAGIC  * Phone Number Prefix - 3 digits
# MAGIC  * Phone Number Remaining - 4 digits
# MAGIC  * All the 4 parts are separated by spaces
# MAGIC * Create a Dataframe with column names name, ssn and phone_number
# MAGIC * Extract last 4 digits from the phone number.
# MAGIC * Extract last 4 digits from SSN.

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

employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )

# COMMAND ----------

from pyspark.sql.functions import col,substring

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employeesDF.select('employee_id','phone_number','ssn').\
    withColumn("phone_last4", substring('phone_number', -4, 4).cast('int')).\
    withColumn("ssn_last4", substring('ssn', -4, 4).cast('int')).\
    show()

# COMMAND ----------

