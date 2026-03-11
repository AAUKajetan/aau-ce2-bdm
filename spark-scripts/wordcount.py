from pyspark.sql import SparkSession
import time
import re

# Initialize Spark
spark = SparkSession.builder \
    .appName("WikiWordCount-v1") \
    .getOrCreate()
sc = spark.sparkContext

# Start timer
t0 = time.time()

# Read the Wikipedia XML file from HDFS
text_rdd = sc.textFile("hdfs:///data/wiki/simplewiki-latest-pages-articles.xml")

# Word count pipeline:
#   Strip XML tags, split each line into words
#   map:     emit (word, 1) pairs
#   reduceByKey: sum counts per word (local combine + shuffle)
word_counts = (
    text_rdd
    .map(lambda line: re.sub(r"<[^>]+>", " ", line))   # remove XML tags
    .flatMap(lambda line: line.lower().split())
    .filter(lambda word: re.match(r"^[a-z]{3,}$", word))  # letters only, min 3 chars
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
)

# Action: sort by count descending, take top 30
top_30 = word_counts.sortBy(lambda x: -x[1]).take(30)

elapsed = time.time() - t0

print(f"\n{'='*50}")
print(f"  Top 30 words (completed in {elapsed:.1f}s)")
print(f"{'='*50}")
for word, count in top_30:
    print(f"  {word:<20} {count:>12,}")

spark.stop()
