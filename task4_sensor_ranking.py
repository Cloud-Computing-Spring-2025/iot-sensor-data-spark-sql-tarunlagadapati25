from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Create Spark session
spark = SparkSession.builder.appName("IoT Sensor Analysis - Task 4").getOrCreate()

# Load and prepare the data
df = spark.read.csv("input/sensor_data.csv", header=True, inferSchema=True)
df = df.withColumn("timestamp", to_timestamp("timestamp"))
df.createOrReplaceTempView("sensor_readings")

# SQL with window function to rank sensors
ranked_sensors = spark.sql("""
    SELECT sensor_id,
           ROUND(AVG(temperature), 2) AS avg_temp,
           DENSE_RANK() OVER (ORDER BY AVG(temperature) DESC) AS rank_temp
    FROM sensor_readings
    GROUP BY sensor_id
""")

# Filter top 5 ranked sensors
top_5 = ranked_sensors.filter("rank_temp <= 5")
top_5.show()

# Save to CSV
top_5.write.mode("overwrite").option("header", True).csv("output/task4_output")
