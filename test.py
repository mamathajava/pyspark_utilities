from pyspark.sql import SparkSession
import os
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

if __name__ == '__main__':
    main()


