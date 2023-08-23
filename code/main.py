import boto3
import pandas as pd
import psycopg2
import io
import csv
from io import StringIO

def lambda_handler(event, context):
    db_params = {
    "host": "apprentice-training-2023-rds.cth7tqaptja4.us-west-1.rds.amazonaws.com",
    "database": "postgres",
    "user": "postgres",
    "password": "hello123"
}

    # Establish a connection to PostgreSQL
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    bucket_name = 'apprentice-training-data-rojesh-dev-2'
    object_key = 'news_sources_cleaned.csv'
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    data = response['Body'].read().decode('utf-8')
    
    # Parse data as CSV
    csv_data = StringIO(data)
    csv_reader = csv.reader(csv_data)
    next(csv_reader)
    
    values_to_insert = []
    for row in csv_reader:
        id_value = row[0]
        name = row[1]
        description = row[2]
        url = row[3]
        category = row[4]
        country = row[5]
    
        values_to_insert.append((id_value, name, description, url, category, country))
    
    placeholders = ', '.join(['%s'] * len(values_to_insert[0]))
    sql = f"INSERT INTO rojesh_etl_assignment(id, name, description, url, category, country) VALUES ({placeholders})"
    
    cursor.executemany(sql, values_to_insert)
    
    conn.commit()
    cursor.close()
    conn.close()
    return {
        'statusCode': 200,
        'body': 'Data cleaned and stored successfully!'
    }