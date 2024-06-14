from pyspark.sql import SparkSession
import os
#from util import get_spark_session
def get_spark_session(env,app_name):
    if env =='DEV':
        spark = SparkSession. \
            builder. \
            master("local"). \
            getOrCreate()
        return spark
    return
def main():
    env = 'DEV'
    spark = get_spark_session(env,'github activity - getting started')
    spark.sql('select current_date').show()
    source_file_df=spark.read \
        .format("csv")\
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("C:\welcome\practice\glue-full-course-main\customer.csv")
    #source_file_df=spark.read.csv("C:\welcome\practice\glue-full-course-main\customer.csv")
    source_file_df.show()
    source_file_df.printSchema()
    source_file_df.createTempView('mam_view')
    full_name_df=spark.sql('select FirstName, LastName from mam_view')
    full_name_df.printSchema()
    full_name_df.show()
    full_name_df.write.format("csv").mode("overwrite").options(header='true').save("C:\AWS_Data_Engineering\ds")

    #spark.sql()


if __name__ == '__main__':
    main()