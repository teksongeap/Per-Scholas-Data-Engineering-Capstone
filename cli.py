import argparse
from mysql_db_interactions import list_transactions
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
    else:
        parser.print_help()
        
if __name__ == "__main__":
    main()
        