from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Aula Interativa 2 - Spark SQL") \
        .getOrCreate()

titanic_df = spark.read.csv('titanic.csv', header='True', inferSchema='True')

#titanic_df.printSchema()

#print( titanic_df.count() )

#titanic_df.groupBy('survived').count().show()

titanic_df.createOrReplaceTempView('titanic_table')
spark.sql("SELECT survived, count(*) FROM titanic_table GROUP BY survived").show()