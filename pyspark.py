from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf


def get_grade(value):
    if value <= 50 and value >= 0:
        return "健康"
    elif value <= 100:
        return "中等"
    elif value <= 150:
        return "对敏感人群不健康"
    elif value <= 200:
        return "不健康"
    elif value <= 300:
        return "非常不健康"
    elif value <= 500:
        return "危险"
    elif value > 500:
        return "爆表"
    else:
        return None

if __name__ == '__main__':
    spark = SparkSession.builder.appName("project").getOrCreate()

    df2015 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("data/Beijing_2015_HourlyPM25_created20160201.csv")
    df2016 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("data/Beijing_2016_HourlyPM25_created20170201.csv")
    df2017 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("data/Beijing_2017_HourlyPM25_created20170803.csv")


    grade_function_udf = udf(get_grade(),StringType())

    # 进来一个Value，出去一个Grade
    grade2015 = df2015.withColumn("Grade",grade_function_udf(df2015['Value'])).groupBy("Grade").count()
    grade2016 = df2015.withColumn("Grade",grade_function_udf(df2016['Value'])).groupBy("Grade").count()
    grade2017 = df2015.withColumn("Grade",grade_function_udf(df2017['Value'])).groupBy("Grade").count()

    # grade2015.select("Grade", "count", grade2015['count'] / df2015.count()).show()
    # grade2016.select("Grade", "count", grade2016['count'] / df2016.count()).show()
    # grade2017.select("Grade", "count", grade2017['count'] / df2017.count()).show()


    result2015 = grade2015.select("Grade", "count").withColumn("precent", grade2015['count'] / df2015.count() * 100)
    result2016 = grade2016.select("Grade", "count").withColumn("precent", grade2016['count'] / df2016.count() * 100)
    result2017 = grade2017.select("Grade", "count").withColumn("precent", grade2017['count'] / df2017.count() * 100)

    result2015.selectExpr("Grade as grade", "count", "precent").write.format("org.elasticsearch.spark.sql").option("es.nodes","192.168.199.61:9200").mode("overwrite").save("weather2017/pm")
    result2016.selectExpr("Grade as grade", "count", "precent").write.format("org.elasticsearch.spark.sql").option("es.nodes","192.168.199.61:9200").mode("overwrite").save("weather2017/pm")
    result2017.selectExpr("Grade as grade", "count", "precent").write.format("org.elasticsearch.spark.sql").option("es.nodes","192.168.199.61:9200").mode("overwrite").save("weather2017/pm")

    spark.stop()