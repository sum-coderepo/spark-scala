import subprocess
from pyspark.sql import SparkSession, Row,SQLContext
from pyspark.sql import functions as F
import sys

def main():
    spark = SparkSession.builder.enableHiveSupport().master("local").appName("first_app").getOrCreate()
    df = spark.sparkContext.parallelize([
        ['20170101', 258, 1003],
        ['20170102', 258, 13],
        ['20170103', 258, 1],
        ['20170104', 258, 108],
        ['20170109', 258, 25],
        ['20170101', 2813, 503],
        ['20170102', 2813, 139],
        ['20170101', 4963, 821],
        ['20170102', 4963, 450]]).toDF(('date', 'ID', 'count'))
    df.show()
    log_txt=spark.sparkContext.textFile("C:\\Users\\suagrawa\\workspace\\SCE_ENGG\\src\\main\\scala\\com\\MOC\\results.txt")
    list_elements = log_txt.collect()
    for element in list_elements:
        print(element)
if __name__ == '__main__':
    main()