from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import argparse

load_dotenv()

def create_database_engine(db_url=None):
    try:
        if db_url.startswith('jdbc:'):
            db_url = db_url.replace('jdbc:', '')
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        print(f"Error creating database engine: {e}")
        return None

def load_data(file_path, db_url, table_name="weather_data"):
    engine = create_database_engine(db_url)
    
    if engine is None:
        print("Database engine not created. Exiting.")
        return
    
    spark = SparkSession.builder.appName("WeatherFlow").getOrCreate()
    try:
        schema = StructType([
          StructField("city", StringType(), True),
          StructField("temperature_k", DoubleType(), True),
          StructField("humidity", LongType(), True),
          StructField("weather_desc", StringType(), True),
          StructField("timestamp", LongType(), True),
          StructField("pressure", LongType(), True),
          StructField("wind_speed", DoubleType(), True),
          StructField("temperature_c", DoubleType(), True),
          StructField("datetime", DateType(), True),
          StructField("wind_category", StringType(), True),
      ])
        df = spark.read.csv(file_path, schema=schema)
        df_pd = df.toPandas()
        
        df_pd.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Data loaded into {table_name} table successfully.")
    except Exception as e:
        print(f"Error loading data to database: {e}")
    finally:
        spark.stop()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load transformed Parquet data to PostgreSQL.')
    parser.add_argument('--input-path', required=True,
                       help='Path to transformed file (e.g., /app/data/transformed/transformed_weather.parquet)')
    parser.add_argument('--table-name', default="weather_data",
                       help='Target table name in PostgreSQL')
    args = parser.parse_args()

    db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@postgres:5432/{os.getenv('DB_NAME')}"

    load_data(args.input_path, db_url, args.table_name)