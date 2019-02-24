from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *





if __name__ == '__main__':
    spark = SparkSession.builder.appName("spark").getOrCreate()

    """
    option("header","true")  显示头信息
    option("inferSchema","true") 自动推到数据
    """
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        "D:\Beijing_2017_HourlyPM25_created20170803.csv")
    df.show()
    df.printSchema()
    data2017 = df.select("Year", "Month", "Day", "Hour","Value", "QC Name")
    data2017.show()
    data2017.printSchema()


    def get_grade(Value):
        if Value >= 0 and Value <= 50:
            return "健康"
        elif Value <= 100:
            return "中等"
        elif Value <= 150:
            return "对敏感人群不健康"
        elif Value <= 200:
            return "不健康"
        elif Value <= 300:
            return " 非常不健康"
        elif Value <= 500:
            return " 危险"
        elif Value >= 501:
            return "爆表"

    #get_grade不需要带括号
    grade_function_udf = udf(get_grade,StringType())

    # 进来一个Value，出去一个grade
    data2017.withColumn("Grade",grade_function_udf(data2017["Value"])).show()


    grade2017=data2017.withColumn("Grade",grade_function_udf(data2017["Value"])).groupBy("Grade").count()

    grade2017.show()
    grade2017.select("Grade","count",grade2017['count']/data2017.count()).show()

    spark.stop()


