from pyspark.sql import SparkSession
import requests
from pyspark.sql.functions import col

# Hardcoded credentials (BAD)
API_KEY = "12345-SECRET-KEY"
DB_PASSWORD = "supersecretpassword"

# Create Spark session
spark = SparkSession.builder.appName("WorstCodeEver").getOrCreate()

# Read CSV without schema and no error handling
df = spark.read.csv("data.csv", header=True)

# Print schema too many times (unnecessary)
print("Schema:")
print(df.printSchema())
print("Schema again:")
print(df.printSchema())

# Collect all data to driver (BAD for big data)
data = df.collect()
print("Collected data:", data)

# Use Python loop instead of Spark transformations
filtered_data = []
for row in data:
    print("Processing row:", row)  # Too many print statements
    try:
        if int(row[2]) > 100:  # Hardcoded index
            filtered_data.append(row)
    except:
        print("Error occurred but ignoring it!")  # No proper error handling

# Convert back to DataFrame (inefficient)
df2 = spark.createDataFrame(filtered_data)

# Call external API for each row without retries (BAD)
for row in filtered_data:
    response = requests.post("https://example.com/api", data={"key": API_KEY, "value": row[1]})
    print("API response:", response.text)

# Cache unnecessarily
df2.cache()

# Trigger multiple actions without reason
print("Row count:", df2.count())
print("Row count again:", df2.count())

# Write output without specifying mode
df2.write.csv("output.csv")
