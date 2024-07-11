from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, concat, lit
import matplotlib.pyplot as plt
import seaborn as sns

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CreditCardAnalysis") \
    .getOrCreate()

# MySQL connection properties
mysql_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
    "user": "root",
    "password": "password"
}

# Load data from MySQL tables into Spark DataFrames
branch_df = spark.read.jdbc(
    url=mysql_properties["url"],
    table="CDW_SAPP_BRANCH",
    properties=mysql_properties
)
branch_df.createOrReplaceTempView("CDW_SAPP_BRANCH")

credit_card_df = spark.read.jdbc(
    url=mysql_properties["url"],
    table="CDW_SAPP_CREDIT_CARD",
    properties=mysql_properties
)
credit_card_df.createOrReplaceTempView("CDW_SAPP_CREDIT_CARD")

customer_df = spark.read.jdbc(
    url=mysql_properties["url"],
    table="CDW_SAPP_CUSTOMER",
    properties=mysql_properties
)
customer_df.createOrReplaceTempView("CDW_SAPP_CUSTOMER")

# -----3.1-----
# PySpark query to get transaction counts by type
transaction_type_df = spark.sql("""
SELECT TRANSACTION_TYPE, COUNT(*) as TRANSACTION_COUNT
FROM CDW_SAPP_CREDIT_CARD
GROUP BY TRANSACTION_TYPE
ORDER BY TRANSACTION_COUNT DESC
""")

# Convert to Pandas for visualization
transaction_type_pd = transaction_type_df.toPandas()

# Calculate percentages
total = transaction_type_pd['TRANSACTION_COUNT'].sum()
transaction_type_pd['PERCENTAGE'] = transaction_type_pd['TRANSACTION_COUNT'] / total * 100

# Create pie chart (same as before)
plt.figure(figsize=(14, 10))
plt.pie(transaction_type_pd['TRANSACTION_COUNT'], 
        labels=transaction_type_pd['TRANSACTION_TYPE'], 
        autopct=lambda pct: f'{pct:.1f}%\n({int(pct*total/100):,})',
        startangle=0, 
        colors=sns.color_palette("pastel"), 
        explode=[0.1 if i == 0 else 0 for i in range(len(transaction_type_pd))])

plt.title('Distribution of Transaction Types in Credit Card Database', fontsize=18, pad=20)
plt.legend(transaction_type_pd['TRANSACTION_TYPE'], title="Transaction Types", 
           loc="center left", bbox_to_anchor=(1, 0, 0.5, 1),
           fontsize=10, title_fontsize=12)

plt.savefig('visualizations/3.1_transaction_type_pie_chart.png', dpi=300, bbox_inches='tight')
plt.close()

print("Plot saved as '3.1_transaction_type_pie_chart.png'")

# -----3.2-----
# PySpark query to get top 10 states with highest number of customers
customer_number_df = spark.sql("""
SELECT CUST_STATE, COUNT(*) AS CUSTOMER_COUNT
FROM CDW_SAPP_CUSTOMER
GROUP BY CUST_STATE
ORDER BY CUSTOMER_COUNT DESC
LIMIT 10
""")

customer_number_pd = customer_number_df.toPandas()

# Create the bar plot (same as before)
plt.figure(figsize=(14,7))
ax = sns.barplot(x='CUST_STATE', y='CUSTOMER_COUNT', data=customer_number_pd)
plt.title('Top 10 States with Highest Number of Customers', fontsize=18)
plt.xlabel('State', fontsize=12)
plt.ylabel('Number of Customers', fontsize=12)

for i, v in enumerate(customer_number_pd['CUSTOMER_COUNT']):
    ax.text(i, v, str(v), ha='center', va='bottom')

plt.savefig('visualizations/3.2_top_10_states_by_customer_count.png', dpi=300)
plt.close()

print("Plot saved as '3.2_top_10_states_by_customer_count.png'")

# -----3.3-----
top_10_customers_df = spark.sql("""
SELECT 
    c.FIRST_NAME, 
    c.LAST_NAME, 
    SUM(t.TRANSACTION_VALUE) AS TOTAL
FROM CDW_SAPP_CUSTOMER c
JOIN CDW_SAPP_CREDIT_CARD t ON c.SSN = t.CUST_SSN
GROUP BY
    c.SSN,
    c.FIRST_NAME,
    c.LAST_NAME
ORDER BY TOTAL DESC
LIMIT 10
""")

top_10_customers_pd = top_10_customers_df.toPandas()
top_10_customers_pd['FULL_NAME'] = top_10_customers_pd['FIRST_NAME'] + ' ' + top_10_customers_pd['LAST_NAME']

plt.figure(figsize=(14, 7))
ax = sns.barplot(x='FULL_NAME', y='TOTAL', data=top_10_customers_pd)
plt.title('Top 10 Customers by Total Transaction Amount', fontsize=18)
plt.xlabel('Customer Name', fontsize=12)
plt.ylabel('Total Transaction Amount ($)', fontsize=12)
plt.xticks(rotation=45, ha='right')

for i, v in enumerate(top_10_customers_pd['TOTAL']):
    ax.text(i, v, f'${v:,.0f}', ha='center', va='bottom')

plt.tight_layout()

plt.savefig('visualizations/3.3_top_10_customers_by_total_transaction_amt.png', dpi=300)
plt.close()

print("Plot saved as '3.3_top_10_customers_by_total_transaction_amt.png'")

# Stop the SparkSession
spark.stop()