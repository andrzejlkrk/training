import sys
reload(sys)
sys.setdefaultencoding("utf8")
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from bs4 import BeautifulSoup
import logging

sourcePath = "/Users/andrzej.lewcun/Desktop/szkolenie/wechat_test/pages"
resultsPath = "/Users/andrzej.lewcun/Desktop/results/pages"

conf = SparkConf().setMaster("local").setAppName("Andrzej")
sc = SparkContext(conf = conf)

webSchema = StructType([
        StructField("date", StringType(), True),
        StructField("url", StringType(), True),
        StructField("text", StringType(), True)])

text = sc.wholeTextFiles(sourcePath)
divided = text.flatMap(lambda x: x[1].split("</ID>"))
spark = SparkSession.builder.appName("Pages").getOrCreate()

def htmlDecomposed(m):
    x = m
    soup = BeautifulSoup(x)
    for script in soup(["script", "style"]):
        script.extract()
    return str(soup)

def getTxt(x):
    soup = BeautifulSoup(x)
    result =('','','')
    try:
        result = (soup.date.get_text(), soup.url.get_text(), soup.body.get_text("|", strip=True))
    except AttributeError:
        logging.info("AttributeError")
    return result

rdd = divided.map(htmlDecomposed).map(getTxt)
df = spark.createDataFrame(rdd, webSchema)
df.write.option("delimiter", "\t").option("header","true").csv(resultsPath, compression="gzip")

df.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/andrzej").option("mode","append")\
.option("dbtable","andrzej.pages").option("driver","com.mysql.cj.jdbc.Driver").option("user", "root").option("password","root123$").save()
