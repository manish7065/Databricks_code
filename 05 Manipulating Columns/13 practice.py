# Databricks notebook source
# MAGIC %md
# MAGIC Date and Time arithmetic

# COMMAND ----------

datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]

# COMMAND ----------

datetimeDF = spark.createDataFrame(datetimes, schema='date STRING, time STRING')

# COMMAND ----------

datetimeDF.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import date_add, date_sub

# COMMAND ----------

help(date_add)

# COMMAND ----------

help(date_sub)

# COMMAND ----------

datetimeDF.withColumn("date_add_date",date_add("date",10)).show(truncate=False)

# COMMAND ----------

datetimeDF.withColumn("date_add_time",date_add("time",10)).show(truncate=False)


# COMMAND ----------

datetimeDF. \
    withColumn("date_add_date", date_add("date", 10)). \
    withColumn("date_add_time", date_add("time", 10)). \
    withColumn("date_sub_date", date_sub("date", 10)). \
    withColumn("date_sub_time", date_sub("time", 10)). \
    show()

# COMMAND ----------

datetimeDF.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp, datediff

# COMMAND ----------

datetimeDF.withColumn('dateddiff_date', datediff(current_date(),"date")).show(truncate=False)

# COMMAND ----------




# COMMAND ----------

datetimeDF. \
    withColumn("datediff_date", datediff(current_date(), "date")). \
    withColumn("datediff_time", datediff(current_timestamp(), "time")). \
    show()

# COMMAND ----------

from pyspark.sql.functions import months_between, add_months,round

# COMMAND ----------

datetimeDF.withColumn("months_between_date",round(months_between(current_date(),"date"),2))\
.withColumn("months_between_time",round(months_between(current_timestamp(),"time"),2))\
.withColumn("add_months_date",add_months('date',3))\
.withColumn("add_months_time",add_months('time',3))\
.show(truncate=False)


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from datetime import date

def calculate_age(dob):
    """
    Calculate the age in years, months, and days based on the given date of birth.
    """
    today = date.today()
    age_year = today.year - dob.year
    age_month = today.month - dob.month
    age_day = today.day - dob.day

    # Adjust the age if the birthday hasn't occurred yet this year
    if age_month < 0 or (age_month == 0 and age_day < 0):
        age_year -= 1
        age_month += 12
        if age_day < 0:
            age_day += today.day

    return age_year, age_month, age_day


# COMMAND ----------

dob = date(1996, 2, 24)
age_year, age_month, age_day = calculate_age(dob)
print(f"Age: {age_year} years, {age_month} months, {age_day} days")


# COMMAND ----------

