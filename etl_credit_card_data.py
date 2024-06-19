from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, concat, lit, lower, substring, lpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from mysql_db_setup import create_db_and_tables_with_keys

# Create spark session
spark = SparkSession.builder.appName('ETL_credit_card_data').getOrCreate()

# -----EXTRACT-----

# Create schema
customer_schema = StructType([
    StructField("FIRST_NAME", StringType(), True),
    StructField("MIDDLE_NAME", StringType(), True),
    StructField("LAST_NAME", StringType(), True),
    StructField("SSN", IntegerType(), True),
    StructField("CREDIT_CARD_NO", StringType(), True),
    StructField("APT_NO", StringType(), True),
    StructField("STREET_NAME", StringType(), True),
    StructField("CUST_CITY", StringType(), True),
    StructField("CUST_STATE", StringType(), True),
    StructField("CUST_COUNTRY", StringType(), True),
    StructField("CUST_ZIP", StringType(), True),
    StructField("CUST_PHONE", StringType(), True),
    StructField("CUST_EMAIL", StringType(), True),
    StructField("LAST_UPDATED", TimestampType(), True),
    StructField("_corrupt_record", StringType(), True)  # To handle corrupt records
])

branch_schema = StructType([
    StructField("BRANCH_CODE", IntegerType(), True),
    StructField("BRANCH_NAME", StringType(), True),
    StructField("BRANCH_STREET", StringType(), True),
    StructField("BRANCH_CITY", StringType(), True),
    StructField("BRANCH_STATE", StringType(), True),
    StructField("BRANCH_ZIP", StringType(), True),
    StructField("BRANCH_PHONE", StringType(), True),
    StructField("LAST_UPDATED", TimestampType(), True),
    StructField("_corrupt_record", StringType(), True)  
])


credit_schema = StructType([
    StructField("TRANSACTION_ID", IntegerType(), True),
    StructField("DAY", StringType(), True),
    StructField("MONTH", StringType(), True),
    StructField("YEAR", StringType(), True),
    StructField("CREDIT_CARD_NO", StringType(), True),
    StructField("CUST_SSN", IntegerType(), True),
    StructField("BRANCH_CODE", IntegerType(), True),
    StructField("TRANSACTION_TYPE", StringType(), True),
    StructField("TRANSACTION_VALUE", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True)  
])


# Read json into dataframe using respective schema
# Multiline option required for json objects that take up multiple lines
customer_df = spark.read.option("multiline", "true").schema(customer_schema).json('json/cdw_sapp_customer.json')
branch_df = spark.read.option("multiline", "true").schema(branch_schema).json('json/cdw_sapp_branch.json')
credit_df = spark.read.option("multiline", "true").schema(credit_schema).json('json/cdw_sapp_credit.json')

# -----TRANSFORM-----

# Drop corrupted records, if any
customer_df = customer_df.drop("_corrupt_record")
branch_df = branch_df.drop("_corrupt_record")
credit_df = credit_df.drop("_corrupt_record")

# Map the data to required format for database
# Pyspark substring() function is 1-based index, not 0-based, for some reason
customer_df = customer_df.withColumn("FIRST_NAME", initcap(customer_df["FIRST_NAME"])) \
                         .withColumn("MIDDLE_NAME", lower(customer_df["MIDDLE_NAME"])) \
                         .withColumn("LAST_NAME", initcap(customer_df["LAST_NAME"])) \
                         .withColumn("FULL_STREET_ADDRESS", concat(customer_df["STREET_NAME"], lit(", "), customer_df["APT_NO"])) \
                         .withColumn("CUST_PHONE", concat(substring(customer_df["CUST_PHONE"], 1, 3), lit("-"), substring(customer_df["CUST_PHONE"], 4, 4)))

# Drop the extraneous columns
customer_df = customer_df.drop("STREET_NAME", "APT_NO")

# Reorder columns to match specs
customer_df = customer_df.select("SSN", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "CREDIT_CARD_NO", "FULL_STREET_ADDRESS",
                                 "CUST_CITY", "CUST_STATE", "CUST_COUNTRY", "CUST_ZIP", "CUST_PHONE", "CUST_EMAIL", "LAST_UPDATED")

# Use fillna() directly on branch_df, passing a dictionary
branch_df = branch_df.fillna({"BRANCH_ZIP": "99999"})

# Convert BRANCH_PHONE in branch_df to (###)###-#### format
branch_df = branch_df.withColumn("BRANCH_PHONE", concat(lit('('), substring(branch_df["BRANCH_PHONE"], 1, 3), lit(')'), 
                                    substring(branch_df["BRANCH_PHONE"], 4, 3), lit('-'), substring(branch_df["BRANCH_PHONE"], 7, 4)))

# Reorder columns
branch_df = branch_df.select("BRANCH_CODE", "BRANCH_NAME", "BRANCH_STREET", "BRANCH_CITY", "BRANCH_STATE", "BRANCH_ZIP",
                             "BRANCH_PHONE", "LAST_UPDATED")

# Add leading zeros to MONTH and DAY column using lpad() so that it is regular
credit_df = credit_df.withColumn("DAY", lpad(credit_df["DAY"], 2, '0')) \
                     .withColumn("MONTH", lpad(credit_df["MONTH"], 2, '0'))
    
# Convert YEAR, MONTH, and DAY column into TIME_ID using concat()
credit_df = credit_df.withColumn("TIME_ID", concat(credit_df["YEAR"], credit_df["MONTH"], credit_df["DAY"]))

# Drop the extraneous columns
credit_df = credit_df.drop("DAY", "MONTH", "YEAR")

# Rename column
credit_df = credit_df.withColumnRenamed("CREDIT_CARD_NO", "CUST_CC_NO")

# Reorder columns
credit_df = credit_df.select("TRANSACTION_ID", "TIME_ID", "CUST_CC_NO", "CUST_SSN", "BRANCH_CODE", "TRANSACTION_TYPE", "TRANSACTION_VALUE")

# Show preview of DataFrames
print(customer_df.show(truncate=False))
print(branch_df.show(truncate=False))
print(credit_df.show(truncate=False))

# -----LOAD-----

# Create the database and the tables
create_db_and_tables_with_keys("localhost", "root", "password", "creditcard_capstone")

# Arguments for writing to the database using Pyspark
jdbc_url = "jdbc:mysql://localhost:3306/creditcard_capstone"
connection_prop = {
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Function for writing to database using the "append" mode
# Using "overwrite" mode does not work since the tables already have foreign keys, so they can't be dropped
def write_dataframe_to_mysql(df, table_name):
    try:
        df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_prop)
        print(f"Successfully wrote DataFrame to table {table_name}")
    except Exception as e:
        print(f"Error writing DataFrame to table {table_name}: {e}")

# Write DataFrames to MySQL tables with error handling
write_dataframe_to_mysql(customer_df, "CDW_SAPP_CUSTOMER")
write_dataframe_to_mysql(branch_df, "CDW_SAPP_BRANCH")
write_dataframe_to_mysql(credit_df, "CDW_SAPP_CREDIT_CARD")

# customer_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_CUSTOMER", mode="append", properties=connection_prop)
# branch_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_BRANCH", mode="append", properties=connection_prop)
# credit_df.write.jdbc(url=jdbc_url, table="CDW_SAPP_CREDIT_CARD", mode="append", properties=connection_prop)

# Stop the session
spark.stop()