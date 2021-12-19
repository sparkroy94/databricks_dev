# Databricks notebook source
#This notebook was self done
#Add notes at the end regarding the justification for the number of tasks that were triggered in ech stage.

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/skewed_data.csv" , header = True)

# COMMAND ----------

#creating the medium df
emp_df = spark.createDataFrame([(1,'A'), (2,'B'), (3,'C'), (4,'D'), (5,'E'), (6,'F'), (7,'G'), (8,'H'), (9,'I'), (10,'J')] , ['emp_id','emp_Letter'])
emp_df.show()

# COMMAND ----------

from pyspark.sql.functions import *
df.groupBy('emp_id').agg(count('*').alias('Rows per dept_id')).show()
#From above we can see that emp_id 1 has much more records, so if we join by that, shuffle will need all of the records of each join key on one partition, and whichever executor gets that partition will delay.

# COMMAND ----------

#To show real - like situation, we repartition on emp_id so that we get number of records in each partition
df = df.repartition(10,'emp_id')
df1 = df.withColumn('Partition_id',spark_partition_id())
df1.groupBy('Partition_id').agg(count('*').alias('Records per partition')).show()
df1.rdd.getNumPartitions()

# COMMAND ----------

#introducing the random integer between 0 to 9
#notice that for each record the emp_id first digit is consistent, second didgit is random
df_salted = df.withColumn('Random',(rand()*10).cast('int'))
df_salted= df_salted.withColumn('emp_id',concat(col('emp_id'),col('Random'))).drop('Random')
df_salted.show()

# COMMAND ----------

#now if we group by emp_id, we will see there is no skew
df_salted.groupBy('emp_id').agg(count('*').alias('Records per join key')).show()

# COMMAND ----------

# salting and exploding join keys of the medium df
#we have to use a lit otherwise column formation is not possible. It has to be made column literal by using lit
l = [lit(i) for i in range(10)]
emp_df_list = emp_df.withColumn('Salt_list',array(l)) #have to make it array() to make col type
emp_df_list.show()
emp_exploded = emp_df_list.withColumn('Random',explode('Salt_list'))
emp_exploded.show()
#emp_df.show()

# COMMAND ----------

#concatenating random value with join key to remove skewness
emp_salted = emp_exploded.withColumn('emp_id',concat('emp_id','Random')).drop('Salt_list','Random')
emp_salted.show()

# COMMAND ----------

#df_salted.rdd.getNumPartitions()
spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1) #just for here diabling broadcast join to demonstrate as the skew salting demo wont work if its not going for shuffle
spark.conf.get('spark.sql.autoBroadcastJoinThreshold')


# COMMAND ----------

#now the join
df_join_salt = df_salted.join(emp_salted,emp_salted.emp_id == df_salted.emp_id,'inner').drop('emp_salted.emp_id')
df_join_salt.show()
df_join_salt.explain()

# COMMAND ----------

join_skew = df.join(emp_df,df.emp_id==emp_df.emp_id,'inner').drop('emp_df.emp_id')
join_skew.show()

# COMMAND ----------

emp_df.rdd.getNumPartitions()

