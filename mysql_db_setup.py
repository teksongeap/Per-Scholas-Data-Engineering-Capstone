import mysql.connector as dbconnect
from mysql.connector import Error


# Creating database and creating tables must be separate
def create_database(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = dbconnect.connect(
            host=host_name,
            user=user_name,
            password=user_password
        )
        if connection.is_connected():
            cursor = connection.cursor()
            # SQL command for creating the database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            print(f"Database '{db_name}' created successfully")
    except Error as e:
        print(f"Error: '{e}'")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection for database creation is closed")


# Create the database and tables CDW_SAPP_CUSTOMER, CDW_SAPP_CREDITCARD, CDW_SAPP_BRANCH if they don't exist            
def create_db_and_tables_with_keys(host_name, user_name, user_password, db_name):
    # Create the database using the function above
    create_database(host_name, user_name, user_password, db_name)
    
    # Required to work with the 'finally' section
    connection = None
    try:
        # Connect using credentials
        connection = dbconnect.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        # If connected execute following SQL commands
        if connection.is_connected():
            cursor = connection.cursor()
            # SQL commands for create the tables
            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS CDW_SAPP_CUSTOMER (
                               SSN VARCHAR(11) PRIMARY KEY,
                               FIRST_NAME VARCHAR(50),
                               MIDDLE_NAME VARCHAR(50),
                               LAST_NAME VARCHAR(50),
                               CREDIT_CARD_NO VARCHAR(20),
                               FULL_STREET_ADDRESS VARCHAR(120),
                               CUST_CITY VARCHAR(50),
                               CUST_STATE VARCHAR(50),
                               CUST_COUNTRY VARCHAR(50),
                               CUST_ZIP VARCHAR(10),
                               CUST_PHONE VARCHAR(15),
                               CUST_EMAIL VARCHAR(100),
                               LAST_UPDATED TIMESTAMP
                           )
                           """)
            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS CDW_SAPP_BRANCH (
                               BRANCH_CODE INT PRIMARY KEY,
                               BRANCH_NAME VARCHAR(100),
                               BRANCH_STREET VARCHAR(100),
                               BRANCH_CITY VARCHAR(50),
                               BRANCH_STATE VARCHAR(50),
                               BRANCH_ZIP VARCHAR(10),
                               BRANCH_PHONE VARCHAR(15),
                               LAST_UPDATED TIMESTAMP
                           )
                           """)
            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS CDW_SAPP_CREDIT_CARD (
                               TRANSACTION_ID INT PRIMARY KEY,
                               TIME_ID VARCHAR(8),
                               CUST_CC_NO VARCHAR(20),
                               CUST_SSN VARCHAR(11),
                               BRANCH_CODE INT,
                               TRANSACTION_TYPE VARCHAR(50),
                               TRANSACTION_VALUE DOUBLE,
                               FOREIGN KEY (CUST_SSN) REFERENCES CDW_SAPP_CUSTOMER(SSN) ON DELETE CASCADE,
                               FOREIGN KEY (BRANCH_CODE) REFERENCES CDW_SAPP_BRANCH(BRANCH_CODE) ON DELETE CASCADE
                           )
                           """) 
    # If error is found, print it
    except Error as e:
        print(f"Error: '{e}'")
    # Close connection for good hygiene
    finally:
        # Check if connection is None and connection is connected
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection for table creation is closed")


# Test functions
if __name__ == "__main__":
    create_db_and_tables_with_keys("localhost", "root", "password", "creditcard_capstone")