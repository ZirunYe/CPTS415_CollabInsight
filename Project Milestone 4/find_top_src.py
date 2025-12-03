from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/c/Users/hleel/OneDrive/Desktop/415/citation_network.csv")

from pyspark.sql.functions import split, explode, col, trim

edges = df.select(col("id").alias("src"), explode(split(trim(col("references")), ";")).alias("dst"))

# count outgoing edges
edges.groupBy("src").count().orderBy(col("count").desc()).show(20)