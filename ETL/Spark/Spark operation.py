#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pyspark # only run after findspark.init()
from pyspark import SparkContext, SparkConf
conf = SparkConf()
sc = SparkContext(conf=conf)


# In[6]:


from pyspark.sql import SQLContext
sqlContext=SQLContext(sc)
from pyspark.sql.types import IntegerType
import pandas as pd
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.window import Window


# In[10]:



df_pd = pd.DataFrame(
    data={'integers': [1, 2, 3],
     'floats': [-1.0, 0.5, 2.7],
     'integer_arrays': [[1, 2], [3, 4, 5], [6, 7, 8, 9]]}
)
df = sqlContext.createDataFrame(df_pd)
def square(x):
    return x**2

square_udf_int = udf(lambda z: square(z), IntegerType())
(df.select('integers',
              'floats',
              square_udf_int('integers').alias('int_squared'),
              square_udf_int('floats').alias('float_squared'))
    .show())


# In[4]:


data = [('Foo',10,'US',3),                ('Foo',39,'UK',1),                ('Bar',57,'IN',2),                ('Bar',72,'CA',2),                ('Baz',22,'US',6),                ('Baz',23,'UK',6)]
df = sqlContext.createDataFrame(data,['name','age','country','id'])


# In[12]:


from pyspark.sql.types import StringType
sqlContext.registerFunction(
                "RankToFlag", lambda x: "Y" if x > 1 else "N", StringType())


# In[16]:


keyCols = 'country'
orderStr = 'id'
queryStr = 'select *, RankToFlag(ROW_NUMBER() over (partition by '                        + keyCols + ' order by ' + orderStr + ' )) as rank '                                                              ' from df order by ' + orderStr


# In[28]:


df.withColumnRenamed('name', 'firstname').show()


# In[26]:


df.filter(df.name == 'Foo').drop('age').show()


# In[23]:


df2 = df.dropDuplicates(subset=['name'])


# In[25]:


df.drop('id').join(df2.drop('id'),df.name==df2.name,'inner').show()


# In[43]:


df.printSchema()dtypes


# In[29]:


df.select(*(df.columns)).show()


# In[52]:


df.where('country' + "='IN'").show()


# In[ ]:


df.withColumnRenamed('name', 'firstname').show()
df.filter(df.name == 'Foo').drop('age').drop('country').show()
df2 = df.dropDuplicates(subset=['name'])
df.join(df2,df.name==df2.name,'inner').show()
df.where('country '+ "='IN'").show()
df.select('name','age').show()


# In[54]:


df.sort('name','id').show()
#dff = sqlContext.sql(query).fillna('')


# In[56]:


from pyspark.sql.functions import udf, lit, monotonically_increasing_id
df.withColumn("ROW_NUM", monotonically_increasing_id()).show()


# In[ ]:


#collease vs repartition
Node 1 = 1,2,3
Node 2 = 4,5,6
Node 3 = 7,8,9
Node 4 = 10,11,12

Then coalesce down to 2 partitions:

Node 1 = 1,2,3 + (10,11,12)
Node 3 = 7,8,9 + (4,5,6)
coalesce uses existing partitions to minimize the amount of data 
that's shuffled.  repartition creates new partitions and does a full shuffle
coalesce results in partitions with different amounts of data 
(sometimes partitions that have much different sizes) and repartition results in roughly equal sized partitions.

coalesce may run faster than repartition, but unequal sized partitions are generally slower to work with 
than equal sized partitions. You ll usually need to repartition datasets after filtering a large data set
. I ve found repartition to be faster overall because Spark is built to work with equal sized partitions.

repartition - its recommended to use repartition while increasing no of partitions, 
because it involve shuffling of all the data.

coalesce- it’s is recommended to use coalesce while reducing no of partitions


# In[ ]:


#persistance
With cache(), you use only the default storage level MEMORY_ONLY. With persist(), you can specify which storage level you want

MEMORY_ONLY--deserialized,recompute them next time whenever needed
MEMORY_ONLY_SER--serialized,space efficient as compared to deserialized objects,
MEMORY_AND_DISK--stores the excess partition on the disk,
MEMORY_AND_DISK_SER
DISK_ONLY


# In[ ]:


--executor mem=50G
--num-exe =10
--exe-core =4
5 tasks per executor --  . So it’s good to keep the number of cores per executor below that number


# In[ ]:


#word count prob
counts = lines.flatMap(lambda x: x.split(' '))                   .map(lambda x: (x, 1))                   .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))


# In[63]:


a=['a','b']
b=['c','d','e']
c=a+b


# In[64]:


c


# In[65]:


a=[1,2,3,4,1]
z=a[0]
for i in a:
    if i>z:
        z=i
print(z)        


# In[27]:


# df.withColumn(
#             "no_basket",
#             F.when(
#                 F.col("id") != 0,
#                 0
#             ).otherwise(
#                 1
#             )).show()
df.withColumn('age category',F.when((F.col('age')>10)&(F.col('age')<70),'Adult').otherwise('NA')).fillna({"age category":0}).show()


# In[74]:


df.filter(
    F.col('age')>50
).select('name','country').show()


# In[28]:


df.groupBy(df.name).agg(F.sum('age')).show()
#df.withColumn("min age",F.min('age').over(Window.partitionBy('name'))).show()
df.withColumn('sum age',F.sum('age').over(Window.partitionBy('name'))).show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




