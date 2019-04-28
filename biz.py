import sys
reload(sys)
sys.setdefaultencoding("utf8")
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType

sourcePath = "/Users/andrzej.lewcun/Desktop/szkolenie/wechat_test/biz"
resultsPath = "/Users/andrzej.lewcun/Desktop/results/biz"

clicksSchema = StructType([
        StructField("id", StringType(), True),
        StructField("biz_id", StringType(), True),
        StructField("biz_name", StringType(), True),
        StructField("biz_code", StringType(), True),
        StructField("biz_description", StringType(), True),
        StructField("qr_code", StringType(), True),
        StructField("ts", IntegerType(), True)])

spark = SparkSession.builder.appName("Biz").getOrCreate()

df = spark.read.option("sep", "\t").schema(clicksSchema).csv(sourcePath)
df.createOrReplaceTempView("bizTbl")

bizResult = spark.sql("""
SELECT biz_id, biz_description, biz_name FROM bizTbl
""")

bizResult.write.option("delimiter", "\t").option("header","true").csv(resultsPath, compression="gzip")
bizResult.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/andrzej").option("mode","append")\
.option("dbtable","andrzej.biz").option("driver","com.mysql.cj.jdbc.Driver").option("user", "root").option("password","root123$").save()
