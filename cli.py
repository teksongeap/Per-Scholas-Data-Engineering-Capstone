import argparse
from mysql_db_interactions import list_transactions

def main():
    parser = argparse.ArgumentParser(description="Credit Card Database CLI")
    subparsers = parser.add_subparsers(dest='command')
    
    # Get transaction details
    list_transactions_parser = subparsers.add_parser('list_transactions', 
                                                    help="List transactions according to specified zipcode, month, and year \
                                                    e.g. 'cli.py list_transactions --zipcode 12345 --month 6 --year 2021'")
    list_transactions_parser.add_argument('--zipcode', required=True, help="Zipcode to filter transactions, format: #####")
    list_transactions_parser.add_argument('--month', required=True, help="Month to filter transactions, format: ##")
    list_transactions_parser.add_argument('--year', required=True, help="Year to filter transactions, format: ####")
    
    
    args = parser.parse_args()
    
    if args.command == 'list_transactions':
        transactions = list_transactions(args.zipcode, args.month, args.year)
        if not transactions.empty:
            print(transactions)
        else:
            print('No transactions found!')
    else:
        parser.print_help()
        
if __name__ == "__main__":
    main()
        