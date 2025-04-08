from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("IoT Sensor Analysis - Task 2").getOrCreate()

# Load the sensor data and register as temp view (reusing schema from Task 1)
df = spark.read.csv("input/sensor_data.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("sensor_readings")

# 1. Filter out-of-range and in-range rows
out_of_range = spark.sql("""
    SELECT * FROM sensor_readings
    WHERE temperature < 18 OR temperature > 30
""")

in_range = spark.sql("""
    SELECT * FROM sensor_readings
    WHERE temperature >= 18 AND temperature <= 30
""")

print("▶ Out-of-range count:")
out_of_range.count()

print("▶ In-range count:")
in_range.count()

# 2. Group by location and compute avg temperature & humidity
agg_df = spark.sql("""
    SELECT location,
           ROUND(AVG(temperature), 2) AS avg_temperature,
           ROUND(AVG(humidity), 2) AS avg_humidity
    FROM sensor_readings
    GROUP BY location
    ORDER BY avg_temperature DESC
""")

# Show results
agg_df.show()

# 3. Save to CSV
agg_df.write.mode("overwrite").option("header", True).csv("output/task2_output")
