from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import sys

# Initialize Spark session
spark = SparkSession.builder.appName("ReviewsCleaning").getOrCreate()

# Read input arguments
input_path = sys.argv[1]  # gs://.../reviews.csv
bq_output_table = sys.argv[2]  # project_id.reviews_data.cleaned_reviews

# Read data
df = spark.read.option("header", True).csv(input_path)

# Cast rating to integer
df = df.withColumn("reviews.rating", col("reviews.rating").cast("int"))

# Filter out null or empty review text
df_clean = df.filter(col("reviews.text").isNotNull() & (col("reviews.text") != ""))

# Add sentiment column based on rating
df_clean = df_clean.withColumn(
    "sentiment",
    when(col("reviews.rating") >= 4, "positive")
    .when(col("reviews.rating") == 3, "neutral")
    .otherwise("negative")
)

# Optional: Select only useful columns
selected_columns = [
    "id", "brand", "name", "reviews.rating", "reviews.text", "reviews.title", "reviews.username",
    "reviews.date", "reviews.numHelpful", "sentiment"
]

df_selected = df_clean.select(*selected_columns)

# Write to BigQuery
df_selected.write \
    .format("bigquery") \
    .option("table", bq_output_table) \
    .option("temporaryGcsBucket", "reviews-pipeline-data") \
    .mode("overwrite") \
    .save()

print("Cleaned data with sentiment written to BigQuery.")
