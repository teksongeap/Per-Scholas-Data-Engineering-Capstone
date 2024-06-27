import argparse
from mysql_db_interactions import list_transactions, get_customer, update_customer
from datetime import datetime
import pandas as pd

current_year = datetime.now().year

def main():
    # Using argparse to make a quick and easy CLI tool
    parser = argparse.ArgumentParser(description="Credit Card Database CLI")
    subparsers = parser.add_subparsers(dest='command')
    
    # List transaction details command
    list_transactions_parser = subparsers.add_parser('list_transactions', 
                                                    help="List transactions according to specified zipcode, month, and year \
                                                    e.g. 'cli.py list_transactions --zipcode 12345 --month 6 --year 2021'")
    
    # Add the arguments that will be used when entering into the command line
    list_transactions_parser.add_argument('--zipcode', required=True, help="Zipcode to filter transactions, format: #####")
    # arg is by default saved as string, 'type=int' makes it int
    list_transactions_parser.add_argument('--month', required=True, type=int, help="Month to filter transactions, format: ##")
    list_transactions_parser.add_argument('--year', required=True, type=int, help="Year to filter transactions, format: ####")
    
    # Get customer details
    get_customer_parser = subparsers.add_parser('get_customer',
                                                help="Get customer details using first name, last name, and last 4 digits of SSN \
                                                    e.g. 'cli.py get_customer --first Michael --last Jordan --ssn 1234'")
    
    get_customer_parser.add_argument('--first', required=True, help="First name of customer")
    get_customer_parser.add_argument('--last', required=True, help="Last name of customer")
    get_customer_parser.add_argument('--ssn', required=True, help="Last 4 digits of SSN of customer")
    
    # Update customer details
    update_customer_parser = subparsers.add_parser('update_customer',
                                                   help="Update customer details using first name, last name, ssn, and set clause \
                                                       e.g. 'cli.py update_customer --ssn 1234 --update cust_phone 123-4567 --update middle_name jenkins'")
    
    update_customer_parser.add_argument('--first', required=True, help="First name of customer")
    update_customer_parser.add_argument('--last', required=True, help="Last name of customer")
    update_customer_parser.add_argument('--ssn', required=True, help="Last 4 digits of SSN of customer")
    # Argument for taking multiple updates to a single customer
    update_customer_parser.add_argument('--update', required=True, action='append', nargs=2, metavar=('FIELD', 'VALUE'), 
                                        help="Set clause for sql query \
                                            e.g. 'cli.py update_customer --ssn 1234 --update cust_phone 123-4567 --update middle_name jenkins'")
    
    # Parse the args so they can be used within the code
    args = parser.parse_args()
    
    # Section for list_transactions command
    if args.command == 'list_transactions':
        transactions = pd.DataFrame()
        # Check if month and year are not ridiculous numbers, prevents any mess with ValueErrors in the mysql_db_interactions
        if 1 <= args.month <= 12 and args.year <= current_year:
            transactions = list_transactions(args.zipcode, args.month, args.year)
        if not transactions.empty:
            print(transactions)
        else:
            print('No transactions found!')
    
    # Section for get_customer command
    elif args.command == 'get_customer':
        customer = get_customer(args.first, args.last, args.ssn)
        if not customer.empty:
            print(customer)
        else:
            print('No such customer found!')
            
    # Section for update_customer command
    elif args.command == 'update_customer':
        # Check if customer exists first
        customer = get_customer(args.first, args.last, args.ssn)
        if not customer.empty:
            # Use a dictionary comprehension to turn updates in arg.update into a dictionary
            updates = {field: value for field, value in args.update}
            update_customer(args.first, args.last, args.ssn, updates)
        else:
            print('No such customer found!')
    else:
        parser.print_help()
        
if __name__ == "__main__":
    main()
        