from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
import os
import boto3
import json

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType


load_dotenv()

ENDPOINT='http://api.weatherapi.com/v1/current.json'
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS = os.getenv("AWS_SECRET_ACCESS")

PARAMS = {
    'key': WEATHER_API_KEY,
    'q': '-30.0368,-51.2090'
    }


@dag(dag_id="weather_etl", schedule_interval="@hourly", start_date=days_ago(1), catchup=False, tags=["weather", "etl"])
def weather_etl():

    @task
    def extract():
        response = requests.get(ENDPOINT, PARAMS)

        if response.status_code != 200:
            raise ValueError(f"Request returned a {response.status_code} code")
        else:
            response_json = response.json()
            return response_json

    @task
    def transform(response_json):
        return {
            "response_json": response_json,
            "localtime": response_json['location']['localtime'].replace(" ", "_").replace(":", "-")
        }
    
    @task
    def load(response_dict):
        # return "s3://bucket/file.json"
        localtime = response_dict['localtime']
        response_json = response_dict['response_json']

        aws_bucket='flight-weather-de'
        aws_region='us-east-1'
        aws_path='weather/data/raw'
        aws_filename=f'{localtime}.json'

        s3 = boto3.client(
            's3',
            region_name=aws_region,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS
        )

        # Upload the file
        s3.put_object(
            Body=json.dumps(response_json),
            Bucket=aws_bucket,
            Key=f'{aws_path}/{aws_filename}'
        )

        return f's3://{aws_bucket}/{aws_path}/{aws_filename}'


    @task_group
    def raw_data():
        response_json = extract()
        response_dict = transform(response_json)
        raw_file_path = load(response_dict)
        return raw_file_path

    @task
    def process_data(raw_file_path):
        print(f"Performing data quality check on {raw_file_path}")

        # Create a SparkSession
        spark = SparkSession.builder \
            .appName("WeatherBatchETL") \
            .config("spark.jars", "file:/opt/bitnami/spark/jars/aws-java-sdk-1.11.995.jar,file:/opt/bitnami/spark/jars/hadoop-aws-3.3.1.jar") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
            .getOrCreate()

        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS)
        spark._jsc.hadoopConfiguration().set(
            "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        
        # Filesystem definitions:
        file_name = raw_file_path.split("/")[-1]
        aws_bucket = "s3a://flight-weather-de"

        processed_folder = aws_bucket + "/weather/data/processed/"

        # Extract:
        df = spark.read.option("multiline", "true").json(f"{aws_bucket}/weather/data/raw/{file_name}")

        # Print the schema to check the structure of the DataFrame
        df.printSchema()

        # Flatten the nested structure
        flattened_df = df.select(
            "current.cloud",
            "current.condition.code",
            "current.condition.icon",
            "current.condition.text",
            "current.feelslike_c",
            "current.feelslike_f",
            "current.gust_kph",
            "current.gust_mph",
            "current.humidity",
            "current.is_day",
            "current.last_updated",
            "current.last_updated_epoch",
            "current.precip_in",
            "current.precip_mm",
            "current.pressure_in",
            "current.pressure_mb",
            "current.temp_c",
            "current.temp_f",
            "current.uv",
            "current.vis_km",
            "current.vis_miles",
            "current.wind_degree",
            "current.wind_dir",
            "current.wind_kph",
            "current.wind_mph",
            "location.country",
            "location.lat",
            "location.localtime",
            "location.localtime_epoch",
            "location.lon",
            "location.name",
            "location.region",
            "location.tz_id"
        )

        # Show the flattened DataFrame schema
        flattened_df.printSchema()

        # Write to parquet
        flattened_df.write.mode("overwrite").parquet(processed_folder + file_name.split(".")[0] + '.parquet')

        # Print the DataFrame
        flattened_df.show()

        # Stop the SparkSession
        spark.stop()

    @task_group
    def processed_data(raw_file_path):
        process_data(raw_file_path)

    result_of_raw_data = raw_data()
    result_of_raw_data >> processed_data(result_of_raw_data)



dag = weather_etl()