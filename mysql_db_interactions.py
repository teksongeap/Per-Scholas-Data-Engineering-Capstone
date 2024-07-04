import mysql.connector as dbconnect
from mysql.connector import Error
from datetime import datetime, timedelta
import pandas as pd
import re

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

# Transactions
# Get month range, returns tuple of start and end date of a chosen month in a chosen year
def get_month_range(month, year):
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
    
    return (start_time_id, end_time_id)

    
# Returns results of sql query for transactions from creditcard_capstone db, order by desc
def list_transactions(zipcode, month, year):
    connection = create_connection()
    cursor = connection.cursor()
    
    month_range = get_month_range(month, year)

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
                AND t.TIME_ID BETWEEN %s AND %s
            ORDER BY
                t.TIME_ID DESC;
            """
    
    # Use the '*' operator to unpack into remaining '%s' slots
    cursor.execute(query, (zipcode, *month_range))
    
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

# Customers
# Returns results of sql query for customers from creditcard_capstone db
def get_customer(firstname, lastname, last_4_ssn):
    connection = create_connection()
    cursor = connection.cursor()
    query = """
            SELECT 
                * 
            FROM 
                CDW_SAPP_CUSTOMER c 
            WHERE 
                FIRST_NAME = %s
                AND LAST_NAME = %s 
                AND RIGHT(SSN, 4) = %s;
            """
    cursor.execute(query, (firstname, lastname, last_4_ssn))
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
    return df

# -----FUNCTIONS FOR FORMATTING INPUT-----
def format_name(name):
    return name.title()

def format_middle_name(name):
    return name.lower()

def validate_cc(cc):
    if not re.match(r'^\d{16}$', cc):
        raise ValueError("CC number must be 16 digits")
    return cc

def format_address(address):
    return address.title()

def format_city(city):
    return city.title()

def validate_state(state):
    if not re.match(r'^[A-Z]{2}$', state):
        raise ValueError("State must be 2 uppercase letters")
    return state

def validate_country(country):
    valid_countries = ['United States', 'Canada', 'Mexico']
    if country not in valid_countries:
        raise ValueError(f"Country must be one of: {', '.join(valid_countries)}")
    return country

def validate_zip(zip_code):
    if not re.match(r'^\d{5}$', zip_code):
        raise ValueError("Zipcode must be 5 digits")
    return zip_code

def format_phone(phone):
    digits = re.sub(r'\D', '', phone)
    if len(digits) !=7:
        raise ValueError("Phone number must have 7 digits")
    return f"{digits[:3]}-{digits[3:]}"

def validate_email(email):
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        raise ValueError("Invalid email format")
    return email


# Checks for correct input and then attempts sql update of chosen customer
def update_customer(firstname, lastname, last_4_ssn, updates):
     # Format and validate inputs
    formatted_updates = {}
    for key, value in updates.items():
        lower_key = key.lower()  # Convert key to lowercase
        if lower_key in ['first_name', 'last_name']:
            formatted_updates[lower_key] = format_name(value)
        elif lower_key == 'middle_name':
            formatted_updates[lower_key] = format_middle_name(value)
        elif lower_key == 'credit_card_no':
            formatted_updates[lower_key] = validate_cc(value)
        elif lower_key == 'full_street_address':
            formatted_updates[lower_key] = format_address(value)
        elif lower_key == 'cust_city':
            formatted_updates[lower_key] = format_city(value)
        elif lower_key == 'cust_state':
            formatted_updates[lower_key] = validate_state(value)
        elif lower_key == 'cust_country':
            formatted_updates[lower_key] = validate_country(value)
        elif lower_key == 'cust_zip':
            formatted_updates[lower_key] = validate_zip(value)
        elif lower_key == 'cust_phone':
            formatted_updates[lower_key] = format_phone(value)
        elif lower_key == 'cust_email':
            formatted_updates[lower_key] = validate_email(value)
        elif lower_key == 'ssn':
            raise ValueError("SSN cannot be updated")
        else:
            raise ValueError(f"Unknown field: {key}")
    
    # Now, try the connection
    connection = create_connection()
    cursor = connection.cursor()
    try:
        # Create the set clause to be used for updating the customer using a generator expression
        # Generator expression is better since we don't need a list
        # Notice the '%s' being added, this will be used in the cursor.execute() function below
        set_clause = ", ".join(f'{key} = %s' for key in formatted_updates.keys())
        query = f"""
                UPDATE 
                    CDW_SAPP_CUSTOMER
                SET 
                    {set_clause},
                    LAST_UPDATED = %s
                WHERE 
                    FIRST_NAME = %s
                    AND LAST_NAME = %s 
                    AND RIGHT(SSN, 4) = %s;
                """
        # Get current time
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Execute query, using '*' to unpack values of formatted_updates into the '%s' slots created by the gen exp in set_clause
        cursor.execute(query, (*formatted_updates.values(), current_time, firstname, lastname, last_4_ssn))
        
        # Check if any rows were affected
        rows_affected = cursor.rowcount
        
        if rows_affected > 0:
            # Commit required for updating database
            connection.commit()
            print(f"Customer updated successfully. {rows_affected} row(s) affected.")
        else:
            print("No matching customer found or no changes were made.")
    except Error as e:
        print(f"Error: {e}")
    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("Connection to MySQL database closed")

# Get monthly bill for cc number, month and year
def monthly_bill(cc, month, year):
    cc = validate_cc(cc)
    month_range = get_month_range(month, year)
    
    connection = create_connection()
    cursor = connection.cursor()
    
    query = """
            SELECT 
                t.CUST_CC_NO,
                SUM(t.TRANSACTION_VALUE) as "Monthly Bill for Given Month and Year"
            FROM 
                CDW_SAPP_CREDIT_CARD t
            WHERE
                t.CUST_CC_NO = %s
                AND t.TIME_ID BETWEEN %s AND %s
            GROUP BY
                t.CUST_CC_NO;
            """
    
    # Use the '*' operator to unpack into remaining '%s' slots
    cursor.execute(query, (cc, *month_range))
    
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
    
# Returns results of transactions made by customer between two dates, order by desc
def list_transactions_btwn_2_dates(firstname, lastname, last_4_ssn, date1, date2):
    connection = create_connection()
    cursor = connection.cursor()
    
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
                c.FIRST_NAME = %s
                AND c.LAST_NAME = %s 
                AND RIGHT(SSN, 4) = %s
                AND t.TIME_ID BETWEEN %s AND %s
            ORDER BY
                t.TIME_ID DESC;
            """

    cursor.execute(query, (firstname, lastname, last_4_ssn, date1, date2))
    
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

# Test functions
if __name__ == "__main__":
    list_transactions('98908', 5, 2018)
    get_customer('marcel', 'camp', '1007')