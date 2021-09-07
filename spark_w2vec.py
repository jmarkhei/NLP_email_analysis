from pyspark.ml.feature import Binarizer
from pyspark.sql.functions import col

processedDF = spark.read.parquet("/mnt/training/reviews/tfidf.parquet").withColumn(
    "Score", col("Score").cast("double")
)
binarizer = Binarizer(threshold=4.0, inputCol="Score", outputCol="ExcellentReview")
binarizedDF = binarizer.transform(processedDF)