# encoding: utf-8
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import split, regexp_extract, desc

# HTTP​ ​requests​ ​to​ ​the​ ​NASA​ ​Kennedy​ ​Space​ ​Center​ ​WWW​ ​server
# Implementacao das perguntas:
# 1. Numero de hosts unicos.
# 2. O total de erros 404.
# 3. Os 5 URLs que mais causaram erro 404.
# 4. Quantidade de erros 404 por dia.
# 5. O total de bytes retornados.

sc = SparkContext()
sqlContext = SQLContext(sc)

log_file = sqlContext.read.text("/home/gustavo/Documents/jerome/projects/access_log_Aug95")

print log_file.show(5, truncate=False)

split_df = log_file.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                        regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
                        regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('request'),
                        regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status'),
                        regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('reply_size'))
                        
print split_df.show(2, truncate=False)

hosts_gr = split_df.groupby( "host").count()
print hosts_gr.show(20000)

print (split_df.groupby( "status").count().filter("status == 404").sort(desc("count"))).show()

print split_df.groupby("host", "status").count().filter("status == 404").sort(desc("count")).show(5)

print split_df.groupby("status", split_df.timestamp.substr(0,2).alias('dayofmonth')).count()\
              .filter("status == 404").sort('dayofmonth').show(1000)

print split_df.groupBy().sum('reply_size').show()

