from pyspark.sql import SparkSession


def main():
	spark = SparkSession.builder.appName("CSVToParquet").getOrCreate()

	# 1. Read the CSV from HDFS into a DataFrame
	# header=True uses the first row for column names
	# inferSchema=True automatically detects numbers vs. strings
	df = spark.read.csv(
		"hdfs:///user/student/data/mock_transactions.csv",
		header=True,
		inferSchema=True,
	)

	# 2. (Optional) Check that the schema was inferred correctly
	df.printSchema()

	# 3. Write the DataFrame back to HDFS as a Parquet directory
	df.write.mode("overwrite").parquet("hdfs:///user/student/data/mock_transactions.parquet")

	spark.stop()


if __name__ == "__main__":
	main()
