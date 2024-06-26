import mysql.connector as dbconnect
from mysql.connector import Error
from datetime import datetime, timedelta
import pandas as pd

# Creates the connection to mysql
def create_connection():
    try:
        connection = dbconnect.connect(
            host='localhost',
            user='root',
            password='password',
            database='creditcard_capstone'
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error: '{e}'")
        return None

# Returns results of sql query for transactions from creditcard_capstone db
def list_transactions(zipcode, month, year):
    connection = create_connection()
    cursor = connection.cursor()
    
    # Get the range to use with BETWEEN ... AND clause of sql query
    start_date = datetime(year, month, 1)
    # If month is December and thus the next month would be in the next year...
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    # Else proceed as normal, subtract one day from first day of next month, creating the end date for the month range
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        
    # Convert datetime to string
    start_time_id = start_date.strftime('%Y%m%d')
    end_time_id = end_date.strftime('%Y%m%d')
    
    # print("start time:", start_time_id, "end time:", end_time_id, "object type:", type(start_time_id))

    query = """
            SELECT 
                t.TRANSACTION_ID,
                t.TIME_ID,
                t.TRANSACTION_TYPE,
                t.TRANSACTION_VALUE,
                t.CUST_CC_NO,
                t.BRANCH_CODE
            FROM 
                CDW_SAPP_CREDIT_CARD t
            JOIN
                CDW_SAPP_CUSTOMER c ON t.CUST_SSN = c.SSN
            WHERE
                c.CUST_ZIP = %s
                AND t.time_id BETWEEN %s AND %s
            ORDER BY
                t.TIME_ID DESC;
            """
    
    cursor.execute(query, (zipcode, start_time_id, end_time_id))
    
    results = cursor.fetchall()
    
    # Close cursor and connection
    if cursor:
        cursor.close()
    if connection and connection.is_connected():
        connection.close()
        print("Connection to MySQL database closed")

    # Get column names
    column_names = [desc[0] for desc in cursor.description]
    
    # To dataframe
    df = pd.DataFrame(results, columns=column_names)
    # print(df)
    return df

# Returns results of sql query for customers from creditcard_capstone db
def get_customer_details(firstname, lastname):
    connection = create_connection()
    cursor = connection.cursor()
    query = """
            SELECT 
                *
            FROM 
                CDW_SAPP_CUSTOMER c
            WHERE
                c.FIRST_NAME = %s
                AND c.LAST_NAME = %s;
            """
    cursor.execute(query, (firstname, lastname))
    results = cursor.fetchall()
    
    # Close cursor and connection
    if cursor:
        cursor.close()
    if connection and connection.is_connected():
        connection.close()
        print("Connection to MySQL database closed")
    
    # Get column names
    column_names = [desc[0] for desc in cursor.description]
    
    # To dataframe
    df = pd.DataFrame(results, columns=column_names)
    print(df)
    return df

def update_customer():
    pass

# Test functions
if __name__ == "__main__":
    list_transactions('98908', 5, 2018)
    get_customer_details('marcel', 'andreas', 'camp')