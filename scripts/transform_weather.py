from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import json
import argparse

def load_data(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        filename = file_path.split('/')[-1]
        return data, filename
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON from file {file_path}.")
        return None

def transform_weather(data, filename):
    spark = SparkSession.builder.appName("WeatherFlow").getOrCreate()
    
    # Define expected schema
    schema = StructType([
        StructField("city", StringType(), True),
        StructField("temperature_k", DoubleType(), True),
        StructField("humidity", LongType(), True),
        StructField("weather_desc", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("pressure", LongType(), True),
        StructField("wind_speed", DoubleType(), True)
    ])
    
    # Read the JSON file into a DataFrame
    df = spark.createDataFrame(data, schema=schema)
    # Add temperature in Celsius
    df = df.withColumn("temperature_c", (F.col("temperature_k") - 273.15).cast("float"))
    
    # Add datetime to human-readable format
    df = df.withColumn("datetime", F.from_unixtime(F.col("timestamp")))
    
    # Categorize wind speed
    df = df.withColumn("wind_category",
                       F.when(F.col("wind_speed") < 0.5, "Calm")
                       .when(F.col("wind_speed") <= 1.5, "Light Air")
                       .when(F.col("wind_speed") <= 3.3, "Light Breeze")
                       .when(F.col("wind_speed") <= 5.5, "Gentle Breeze")
                       .when(F.col("wind_speed") <= 7.9, "Moderate Breeze")
                       .when(F.col("wind_speed") <= 10.7, "Fresh Breeze")
                       .when(F.col("wind_speed") <= 13.8, "Strong Breeze")
                       .when(F.col("wind_speed") <= 17.1, "Near Gale")
                       .when(F.col("wind_speed") <= 20.7, "Gale")
                       .when(F.col("wind_speed") <= 24.4, "Strong Gale")
                       .when(F.col("wind_speed") <= 28.4, "Storm")
                       .when(F.col("wind_speed") <= 32.6, "Violent Storm")
                       .otherwise("Hurricane"))
    
    
    timestamp = filename.split('_')[-2:]
    timestamp = "".join(timestamp).replace('.json', '')
    transformed_output = f"transformed_weather_{timestamp}.csv"
    df.write.mode("overwrite").csv(transformed_output)
    
    print(f"Transformed data saved to {transformed_output}")
    spark.stop()
    return transformed_output

def main(params):
    data, filename = load_data(params.input_file)
    if data:
        transformed_path = transform_weather(data, filename)
        return transformed_path
    else:
        print("Skipping transformation due to missing/empty input file.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Transform raw weather.')
    parser.add_argument('--input-file', required=True,
                       help='Path to raw JSON file (e.g., /app/data/raw_weather_*.json)')

    args = parser.parse_args()
    
    main(args)