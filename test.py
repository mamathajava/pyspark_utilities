from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import os

from pyspark.sql.functions import col, lit


#from util import get_spark_session
def get_spark_session(env,app_name):
    if env =='DEV':
        spark = SparkSession. \
            builder.\
            master("local").\
            getOrCreate()
        return spark
    return
def main():
    env = 'DEV'
    spark = get_spark_session(env,'github activity - getting started')
    spark.sql('select current_date').show()


    #df=spark.read.csv('C:\welcome\practice\glue-full-course-main\customer.csv')
    df=spark.read.option("header","true")\
        .option("inferSchema","true")\
        .csv('C:\welcome\practice\glue-full-course-main\customer.csv')
    #df.printSchema()
    df=df.withColumn('task',lit('doing'))
    df.createTempView("mamatha_tbl")
    df3=spark.sql("select SalesPerson,Phone from mamatha_tbl")
    dropcolum=df3.drop("phone")
    df.printSchema()
    #df3.write.format("C:\welcome\practice\target_files").mode("overwrite")
    df3.write.mode("overwrite").csv('C:/welcome/practice/results_files')

    dropcolum.show()



#.option("header","true"))
    #df.printSchema()
   # df.show(5)


if __name__ == '__main__':
    main()


