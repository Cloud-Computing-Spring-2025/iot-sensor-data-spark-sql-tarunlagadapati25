from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Create Spark session
spark = SparkSession.builder.appName("IoT Sensor Analysis - Task 3").getOrCreate()

# Load the data
df = spark.read.csv("input/sensor_data.csv", header=True, inferSchema=True)

# Convert timestamp string to proper TimestampType
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Register temp view
df.createOrReplaceTempView("sensor_readings")

# SQL query to extract hour and average temperature per hour
hourly_avg_temp = spark.sql("""
    SELECT HOUR(timestamp) AS hour_of_day,
           ROUND(AVG(temperature), 2) AS avg_temp
    FROM sensor_readings
    GROUP BY hour_of_day
    ORDER BY hour_of_day
""")

# Show results
hourly_avg_temp.show(24)

# Save to CSV
hourly_avg_temp.write.mode("overwrite").option("header", True).csv("output/task3_output")
