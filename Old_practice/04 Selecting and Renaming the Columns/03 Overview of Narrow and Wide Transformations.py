# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview of Narrow and Wide Transformations
# MAGIC 
# MAGIC Let us get an overview of Narrow and Wide Transformations.
# MAGIC * Here are the functions related to narrow transformations. Narrow transformations doesn't result in shuffling. These are also known as row level transformations.
# MAGIC   * `df.select`
# MAGIC   * `df.filter`
# MAGIC   * `df.withColumn`
# MAGIC   * `df.withColumnRenamed`
# MAGIC   * `df.drop`
# MAGIC * Here are the functions related to wide transformations.
# MAGIC   * `df.distinct`
# MAGIC   * `df.union` or any set operation
# MAGIC   * `df.join` or any join operation
# MAGIC   * `df.groupBy`
# MAGIC   * `df.sort` or `df.orderBy`
# MAGIC * Any function that result in shuffling is wide transformation. For all the wide transformations, we have to deal with group of records based on a key.

# COMMAND ----------

# MAGIC %md
# MAGIC In Apache Spark, transformations are operations that transform one RDD (Resilient Distributed Dataset) into another. Transformations in Spark can be categorized as narrow or wide transformations, based on how they operate on the data.
# MAGIC 
# MAGIC A narrow transformation is one that does not require shuffling of data between partitions. In other words, each partition of the input RDD is processed independently, and the output of a narrow transformation for each partition depends only on the data in that partition. Narrow transformations are often fast and efficient, as they can be performed in parallel across multiple partitions.
# MAGIC 
# MAGIC Here's an example of a narrow transformation:
# MAGIC 
# MAGIC python
# MAGIC Copy code
# MAGIC # Example of a narrow transformation: map()
# MAGIC 
# MAGIC # Create an RDD of integers
# MAGIC rdd = sc.parallelize([1, 2, 3, 4, 5])
# MAGIC 
# MAGIC # Apply a map transformation to square each element in the RDD
# MAGIC squared_rdd = rdd.map(lambda x: x ** 2)
# MAGIC In the above example, the map() transformation is a narrow transformation because it is applied to each partition of the RDD independently, and the output of the transformation for each partition depends only on the data in that partition.
# MAGIC 
# MAGIC On the other hand, a wide transformation is one that requires shuffling of data between partitions. In other words, the output of a wide transformation for each partition depends on the data in multiple partitions. Wide transformations are often slower and more expensive than narrow transformations, as they require network communication and data transfer between nodes in the cluster.
# MAGIC 
# MAGIC Here's an example of a wide transformation:
# MAGIC 
# MAGIC python
# MAGIC Copy code
# MAGIC # Example of a wide transformation: groupByKey()
# MAGIC 
# MAGIC # Create an RDD of (key, value) pairs
# MAGIC rdd = sc.parallelize([(1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (2, 'e')])
# MAGIC 
# MAGIC # Apply a groupByKey transformation to group the values by key
# MAGIC grouped_rdd = rdd.groupByKey()
# MAGIC In the above example, the groupByKey() transformation is a wide transformation because it requires data to be shuffled between partitions. The output for each partition depends on the data in multiple partitions, as the keys need to be collected and grouped together.
# MAGIC 
# MAGIC In general, it's recommended to use narrow transformations whenever possible, as they are more efficient and scalable. Wide transformations should be used judiciously, as they can be slow and may require additional resources to handle the shuffle.

# COMMAND ----------

# MAGIC %md
# MAGIC ## just doubleclick to read
# MAGIC Narrow Transformations:
# MAGIC 
# MAGIC map(func): applies the given function to each element in the RDD and returns a new RDD with the transformed values.
# MAGIC flatMap(func): applies the given function to each element in the RDD and returns a new RDD with the flattened results.
# MAGIC filter(func): returns a new RDD with only the elements that satisfy the given predicate function.
# MAGIC mapPartitions(func): applies the given function to each partition of the RDD and returns a new RDD with the transformed partitions.
# MAGIC mapPartitionsWithIndex(func): applies the given function to each partition of the RDD, along with its index, and returns a new RDD with the transformed partitions.
# MAGIC glom(): returns an RDD with the data from each partition collected into a list.
# MAGIC coalesce(numPartitions): returns a new RDD that has been coalesced to the specified number of partitions.
# MAGIC repartition(numPartitions): returns a new RDD that has been repartitioned to the specified number of partitions.
# MAGIC distinct(): returns a new RDD with only the distinct elements from the current RDD.
# MAGIC pipe(command, env=None): applies the given shell command to each partition of the RDD and returns a new RDD with the output.
# MAGIC zip(otherRDD): returns a new RDD by zipping the elements of the current RDD with the corresponding elements from another RDD.
# MAGIC zipWithIndex(): returns a new RDD by zipping the elements of the current RDD with their index values.
# MAGIC zipWithUniqueId(): returns a new RDD by zipping the elements of the current RDD with unique IDs.
# MAGIC Wide Transformations:
# MAGIC 
# MAGIC groupByKey(): groups the values by key in the RDD and returns a new RDD with the grouped values.
# MAGIC reduceByKey(func): applies the given function to all the values that have the same key in the RDD and returns a new RDD with the reduced values for each key.
# MAGIC aggregateByKey(zeroValue, seqFunc, combFunc): applies the given functions to the values that have the same key in the RDD and returns a new RDD with the aggregated values for each key.
# MAGIC sortByKey(ascending): sorts the elements in the RDD by key and returns a new RDD.
# MAGIC join(otherRDD, numPartitions=None): joins the current RDD with another RDD based on their common keys and returns a new RDD with the joined data.
# MAGIC cogroup(otherRDD, numPartitions=None): groups the data from multiple RDDs by key and returns a new RDD with the grouped data.
# MAGIC cartesian(otherRDD): returns a new RDD that contains all possible combinations of elements from the current RDD and another RDD.
# MAGIC subtractByKey(otherRDD, numPartitions=None): returns a new RDD that contains only the elements from the current RDD that do not have a corresponding key in another RDD.
# MAGIC keys(): returns a new RDD with only the keys from the current RDD.
# MAGIC values(): returns a new RDD with only the values from the current RDD.
# MAGIC mapValues(func): applies the given function to each value in the RDD and returns a new RDD with the transformed values.
# MAGIC flatMapValues(func): applies the given function to each value in the RDD and returns a new RDD with the flattened results.
# MAGIC combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions=None): combines the values for each key in

# COMMAND ----------

