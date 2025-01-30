from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Load CSV to BigQuery") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0") \
    .getOrCreate()

# GCS bucket details
bucket_path = "gs://toronto-traffic-ds-landing/*"
_Tmp_Bucket= "gs://samp-temp-bkt/"

# Read the source CSV
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(bucket_path)

# BigQuery configurations
project_id = "ttc-gcp"
dataset = "ttcdataset"

# 1. Events Table
events_df = df.select(
    col("EVENT_UNIQUE_ID"),
    col("OCC_DATE"),
    col("OCC_MONTH"),
    col("OCC_DOW"),
    col("OCC_YEAR").cast("int"),
    col("OCC_HOUR").cast("int"),
    col("DIVISION")
)

events_df.write.format("bigquery") \
    .option("table", f"{project_id}:{dataset}.events") \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .option("temporaryGcsBucket",_Tmp_Bucket) \
    .save()

# 2. Collision Outcomes Table
collision_outcomes_df = df.select(
    col("EVENT_UNIQUE_ID"),
    col("FATALITIES").cast("int"),
    when(col("INJURY_COLLISIONS") == "Yes", True).otherwise(False).alias("INJURY_COLLISIONS"),
    when(col("FTR_COLLISIONS") == "Yes", True).otherwise(False).alias("FTR_COLLISIONS"),
    when(col("PD_COLLISIONS") == "Yes", True).otherwise(False).alias("PD_COLLISIONS")
)

collision_outcomes_df.write.format("bigquery") \
    .option("table", f"{project_id}:{dataset}.collision_outcomes") \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .option("temporaryGcsBucket",_Tmp_Bucket) \
    .save()

# 3. Location Table
location_df = df.select(
    col("HOOD_158"),
    col("NEIGHBOURHOOD_158"),
    col("LONG_WGS84").cast("float"),
    col("LAT_WGS84").cast("float")
).distinct()  # Avoid duplicates for neighborhood data

location_df.write.format("bigquery") \
    .option("table", f"{project_id}:{dataset}.location") \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .option("temporaryGcsBucket",_Tmp_Bucket) \
    .save()

# 4. Collision Details Table
collision_details_df = df.select(
    col("EVENT_UNIQUE_ID"),
    when(col("AUTOMOBILE") == "Yes", True).otherwise(False).alias("AUTOMOBILE"),
    when(col("MOTORCYCLE") == "Yes", True).otherwise(False).alias("MOTORCYCLE"),
    when(col("PASSENGER") == "Yes", True).otherwise(False).alias("PASSENGER"),
    when(col("BICYCLE") == "Yes", True).otherwise(False).alias("BICYCLE"),
    when(col("PEDESTRIAN") == "Yes", True).otherwise(False).alias("PEDESTRIAN")
)

collision_details_df.write.format("bigquery") \
    .option("table", f"{project_id}:{dataset}.collision_details") \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .option("temporaryGcsBucket",_Tmp_Bucket) \
    .save()

# Stop the Spark session
spark.stop()
