import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from mysql_db_setup import create_table_with_primary_key
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv('secrets.env')

host = os.getenv('HOST')
username = os.getenv('USER')
password = os.getenv('PASSWORD')
database = os.getenv('DATABASE')
database_url = os.getenv('DATABASE_URL')

spark = SparkSession.builder.appName('ETL_loan_data_api').getOrCreate()

connection_prop = {
    "user": username,
    "password": password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Fetch json data from api
def fetch_data_from_api(url):
    print("Fetching data from API url:", url)
    response = requests.get(url)
    print('Status code:', response.status_code)
    if response.status_code == 200:
        print("Successfully fetched from API")
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")
    
# Create dataframe from json
def create_df_from_json(json_data):
    # Define the schema
    schema = StructType([
        StructField('Application_ID', StringType(), True),
        StructField('Gender', StringType(), True),
        StructField('Married', StringType(), True),
        StructField('Dependents', StringType(), True),
        StructField('Education', StringType(), True),
        StructField('Self_Employed', StringType(), True),
        StructField('Credit_History', IntegerType(), True),
        StructField('Property_Area', StringType(), True),
        StructField('Income', StringType(), True),
        StructField('Application_Status', StringType(), True),
        StructField("_corrupt_record", StringType(), True)
    ])
    
    # Create dataframe from json
    # df = spark.read.schema(schema).json(spark.sparkContext.parallelize([json_data]))
    df = spark.createDataFrame(json_data, schema)
    
    return df

# write dataframe to mysql use overwrite mode, safe now because there are no foreign keys
def write_df_to_mysql(df, table_name):
    try:
        df.write.jdbc(url=database_url, table=table_name, mode='overwrite', properties=connection_prop)
        print(f"Successfully wrote DataFrame to table {table_name}")
    except Exception as e:
        print(f"Error writing DataFrame to table {table_name}: {e}")


if __name__ == "__main__":
    api_url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"
    table_name = "CDW_SAPP_LOAN_APP"
    try:
        # Fetch data
        json_data = fetch_data_from_api(api_url)
        
        # Create the dataframe
        df = create_df_from_json(json_data)
        
        print(df.show())
        
        df = df.drop("_corrupt_record")
        
        # Create the CDW_SAPP_LOAN_APP table
        create_table_with_primary_key(host, username, password, database)
        
        # Write to table
        write_df_to_mysql(df, table_name)
        
        
    except Exception as e:
        print(F"Error: {e}")
        
    finally:
        spark.stop()