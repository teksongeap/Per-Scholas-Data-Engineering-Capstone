from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, concat, lit, lower, substring, unix_timestamp, lpad, col, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Create spark session
spark = SparkSession.builder.appName('ETL_credit_card_data').getOrCreate()

# ----EXTRACT----

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

# Read json into dataframe using respective schema
# Multiline option required for json objects that take up multiple lines
customer_df = spark.read.option("multiline", "true").schema(customer_schema).json('cdw_sapp_customer.json')
credit_df = spark.read.option("multiline", "true").schema(credit_schema).json('cdw_sapp_credit.json')
branch_df = spark.read.option("multiline", "true").schema(branch_schema).json('cdw_sapp_branch.json')

# ----TRANSFORM----
# Drop corrupted records, if any
customer_df = customer_df.drop("_corrupt_record")
credit_df = credit_df.drop("_corrupt_record")
branch_df = branch_df.drop("_corrupt_record")

# Map the data to required format for database
# Pyspark substring() function is 1-based index, not 0-based, for some reason
customer_df = customer_df.withColumn("FIRST_NAME", initcap(customer_df["FIRST_NAME"])) \
                         .withColumn("MIDDLE_NAME", lower(customer_df["MIDDLE_NAME"])) \
                         .withColumn("LAST_NAME", initcap(customer_df["LAST_NAME"])) \
                         .withColumn("FULL_STREET_ADDRESS", concat(customer_df["STREET_NAME"], lit(", "), customer_df["APT_NO"])) \
                         .withColumn("CUST_PHONE", concat(substring(customer_df["CUST_PHONE"], 1, 3), lit("-"), substring(customer_df["CUST_PHONE"], 4, 4)))

# Add leading zeros to MONTH and DAY column using lpad() so that it is regular
credit_df = credit_df.withColumn("DAY", lpad(credit_df["DAY"], 2, '0')) \
                     .withColumn("MONTH", lpad(credit_df["MONTH"], 2, '0'))
    
# Convert YEAR, MONTH, and DAY column into TIME_ID using concat()
credit_df = credit_df.withColumn("TIME_ID", concat(credit_df["YEAR"], credit_df["MONTH"], credit_df["DAY"]))

# Use fillna() directly on branch_df, passing a dictionary
branch_df = branch_df.fillna({"BRANCH_ZIP": "99999"})

# Convert BRANCH_PHONE in branch_df to (###)###-#### format
branch_df = branch_df.withColumn("BRANCH_PHONE", concat(lit('('), substring(branch_df["BRANCH_PHONE"], 1, 3), lit(')'), 
                                    substring(branch_df["BRANCH_PHONE"], 4, 3), lit('-'), substring(branch_df["BRANCH_PHONE"], 7, 4)))
print(customer_df.show(truncate=False))
print(credit_df.show(truncate=False))
print(branch_df.show(truncate=False))

spark.stop()