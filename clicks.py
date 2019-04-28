import sys
reload(sys)
sys.setdefaultencoding("utf8")
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType

sourcePath = "/Users/andrzej.lewcun/Desktop/szkolenie/wechat_test/clicks"
resultsPath = "/Users/andrzej.lewcun/Desktop/results/clicks"

clicksSchema = StructType([
        StructField("id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("reads", IntegerType(), True),
        StructField("likes", IntegerType(), True),
        StructField("ts", IntegerType(), True)])

spark = SparkSession.builder.appName("Clicks").getOrCreate()

df = spark.read.option("sep", "\t").schema(clicksSchema).csv(sourcePath)
df.createOrReplaceTempView("clicksTbl")

# This solution assumes that one article is represented by exactly one URL

maxReadsLikes = spark.sql("""
SELECT * FROM
(
        SELECT  'reads' AS ranking_type, RANK() OVER(ORDER BY reads_total DESC) AS rank, url, reads_total AS total FROM
                (SELECT url, SUM(reads) AS reads_total FROM clicksTbl
                GROUP BY url) as t1
)
WHERE rank = 1
UNION ALL
SELECT * FROM
(
        SELECT  'likes' AS ranking_type, RANK() OVER(ORDER BY likes_total DESC) AS rank, url, likes_total AS total FROM
                (SELECT url, SUM(likes) AS likes_total FROM clicksTbl 
                GROUP BY url) as t1
)
WHERE rank = 1
""")

maxReadsLikes.write.option("delimiter", "\t").option("header","true").csv(resultsPath, compression="gzip")
maxReadsLikes.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/andrzej").option("mode","append")\
.option("dbtable","andrzej.max").option("driver","com.mysql.cj.jdbc.Driver").option("user", "root").option("password","root123$").save()
