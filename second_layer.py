from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("BreweriesDataTransformation") \
    .getOrCreate()

# Load raw JSON data into DataFrame
raw_brew_data_df = spark.read.json("all_breweries_data.json", multiLine = True)

# Perform transformations 
breweries_df = raw_brew_data_df.select(
    "id", "name", "brewery_type", 
    "address_1", "address_2", "address_3", 
    "city", "state_province", "postal_code", 
    "country", "longitude", "latitude", 
    "phone", "website_url", "state", "street"
)

# Partition data by brewery location
breweries_df.write.partitionBy(
    "country", "id"
).parquet("breweries_location_parquet", mode="overwrite")

# Stop SparkSession
spark.stop()
