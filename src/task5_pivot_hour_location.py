from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, hour, avg, round

# Create Spark session
spark = SparkSession.builder.appName("IoT Sensor Analysis - Task 5").getOrCreate()

# Load data
df = spark.read.csv("input/sensor_data.csv", header=True, inferSchema=True)
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Register temp view
df.createOrReplaceTempView("sensor_readings")

# Step 1: SQL to get avg temperature by location and hour
hourly_avg = spark.sql("""
    SELECT location,
           HOUR(timestamp) AS hour_of_day,
           ROUND(AVG(temperature), 2) AS avg_temp
    FROM sensor_readings
    GROUP BY location, hour_of_day
""")

# Step 2: Pivot using DataFrame API
pivot_df = hourly_avg.groupBy("location") \
    .pivot("hour_of_day", list(range(24))) \
    .agg(avg("avg_temp"))

# Show result
pivot_df.show(truncate=False)

# Save to CSV
pivot_df.write.mode("overwrite").option("header", True).csv("output/task5_output")
