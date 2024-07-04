import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sqlalchemy

# Establish connection to database
engine = sqlalchemy.create_engine('mysql://root:password@localhost/creditcard_capstone')

# -----3.1-----
# sql query to get transaction counts by type
transaction_type_query = """
        SELECT TRANSACTION_TYPE, COUNT(*) as TRANSACTION_COUNT
        FROM CDW_SAPP_CREDIT_CARD
        GROUP BY TRANSACTION_TYPE
        ORDER BY TRANSACTION_COUNT DESC
        """

# Execute transaction_type_query and load results into a DataFrame
transaction_type_df = pd.read_sql(transaction_type_query, engine)

# Calculate percentages
total = transaction_type_df['TRANSACTION_COUNT'].sum()
transaction_type_df['PERCENTAGE'] = transaction_type_df['TRANSACTION_COUNT'] / total * 100

# Set the style and color palette
sns.set_style("whitegrid")
colors = sns.color_palette("pastel")

# Create explode tuple (0.1 for the largest slice, 0 for others)
explode = [0.1 if i == 0 else 0 for i in range(len(transaction_type_df))]

# Create pie chart with explode so that the biggest slice gets emphasized
plt.figure(figsize=(14, 10))
plt.pie(transaction_type_df['TRANSACTION_COUNT'], 
        labels=transaction_type_df['TRANSACTION_TYPE'], 
        autopct=lambda pct: f'{pct:.1f}%\n({int(pct*total/100):,})',
        startangle=0, 
        colors=colors, 
        explode=explode)

plt.title('Distribution of Transaction Types in Credit Card Database', fontsize=18, pad=20)

# Add legend
plt.legend(transaction_type_df['TRANSACTION_TYPE'], title="Transaction Types", 
           loc="center left", bbox_to_anchor=(1, 0, 0.5, 1),
           fontsize=10, title_fontsize=12)

# Save the plot
plt.savefig('visualizations/3.1_transaction_type_pie_chart.png', dpi=300, bbox_inches='tight')
plt.close()

print("Plot saved as '3.1_transaction_type_pie_chart.png'")

# -----3.2-----
# sql query to get top 10 states with highest number of customers
top_10_states_customer_number_query = """
        SELECT CUST_STATE, COUNT(*) AS CUSTOMER_COUNT
        FROM CDW_SAPP_CUSTOMER
        GROUP BY CUST_STATE
        ORDER BY CUSTOMER_COUNT DESC
        LIMIT 10
        """
        
customer_number_df = pd.read_sql(top_10_states_customer_number_query, engine)

# Set the style
sns.set_style("whitegrid")
sns.set_palette("pastel")

# Create the bar plot
plt.figure(figsize=(14,7))
ax = sns.barplot(x='CUST_STATE', y='CUSTOMER_COUNT', data=customer_number_df)
plt.title('Top 10 States with Highest Number of Customers', fontsize=18)
plt.xlabel('State', fontsize=12)
plt.ylabel('Number of Customers', fontsize=12)

for i, v in enumerate(customer_number_df['CUSTOMER_COUNT']):
    ax.text(i,v, str(v), ha='center', va='bottom')
    
# Save plot
plt.savefig('visualizations/3.2_top_10_states_by_customer_count.png', dpi=300)
plt.close()

print("Plot saved as '3.2_top_10_states_by_customer_count.png'")

# -----3.3-----
top_10_customers_query = """
        SELECT 
            c.FIRST_NAME, 
            C.LAST_NAME, 
            SUM(t.TRANSACTION_VALUE) AS TOTAL
        FROM CDW_SAPP_CUSTOMER c
        JOIN CDW_SAPP_CREDIT_CARD t ON c.SSN = t.CUST_SSN
        GROUP BY
            c.SSN,
            c.FIRST_NAME,
            c.LAST_NAME
        ORDER BY TOTAL DESC
        LIMIT 10
""" 

top_10_customer_df = pd.read_sql(top_10_customers_query, engine)
top_10_customer_df['FULL_NAME'] = top_10_customer_df['FIRST_NAME'] + ' ' + top_10_customer_df['LAST_NAME']

sns.set_style('whitegrid')
sns.set_palette('pastel') 

plt.figure(figsize=(14, 7))
ax = sns.barplot(x='FULL_NAME', y='TOTAL', data=top_10_customer_df)
plt.title('Top 10 Customers by Total Transaction Amount', fontsize=18)
plt.xlabel('Customer Name', fontsize=12)
plt.ylabel('Total Transaction Amount ($)', fontsize=12)
plt.xticks(rotation=45, ha='right')

for i, v in enumerate(top_10_customer_df['TOTAL']):
    ax.text(i, v, f'${v:,.0f}', ha='center', va='bottom')

plt.tight_layout()

plt.savefig('visualizations/3.3_top_10_customers_by_total_transaction_amt.png', dpi=300)
plt.close()

print("Plot saved as '3.3_top_10_customers_by_total_transaction_amt.png'")