
import boto3
from moto import mock_aws
import requests
import oracledb
# Function to upload data from URL to S3 bucket
def upload_data_to_s3_from_url(url, bucket_name, object_key):
    # Fetch data from URL
    response = requests.get(url)
    data = response.content

    # Create an S3 client using boto3
    s3 = boto3.client('s3')

    # Create the S3 bucket
    s3.create_bucket(Bucket=bucket_name)

    # Upload data to S3 bucket
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=data)

    print(f"Data from URL '{url}' uploaded to S3 bucket '{bucket_name}' with key '{object_key}'.")
# Function to read data from S3 bucket
def read_data_from_s3(bucket_name, object_key):
    # Create an S3 client using boto3
    s3 = boto3.client('s3')
    # Read data from S3 bucket
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    data = response['Body'].read()
    return data

# URL of the CSV file
url = 'https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2021-financial-year-provisional/Download-data/annual-enterprise-survey-2021-financial-year-provisional-csv.csv'

# Name of the S3 bucket
bucket_name = 'my_bucket'
# Key (object key) under which the file will be stored in the S3 bucket
object_key = 'data.csv'
# Start the moto mock S3 server
with mock_aws():
    # Upload data to S3 from URL
    upload_data_to_s3_from_url(url, bucket_name, object_key)
    # Read the uploaded data from S3
    s3_data = read_data_from_s3(bucket_name, object_key)
# Print the data
print(s3_data.decode('utf-8'))  # Assuming the data is UTF-8 encoded CSV


