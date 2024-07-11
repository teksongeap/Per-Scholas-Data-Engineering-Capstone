from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, desc, substring, concat, lit
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


spark = SparkSession.builder.appName('loan_data_analysis_viz').getOrCreate()



# Load table into dataframe
loan_df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:mysql://localhost:3306/creditcard_capstone') \
    .option('dbtable', 'CDW_SAPP_LOAN_APP') \
    .option('user', 'root') \
    .option('password', 'password') \
    .load()

# -----5.1-----
    
# Calculate percentage of approved applications for self_employed
self_employed = loan_df.filter(col('Self_Employed') =='Yes')
total_self_employed = self_employed.count()
approved_self_employed = self_employed.filter(col('Application_Status') == 'Y').count()
approval_percentage = (approved_self_employed / total_self_employed) * 100

# Prepare data for seaborn
data1 = pd.DataFrame({
    'Status': ['Approved', 'Not Approved'],
    'Percentage': [approval_percentage, 100 - approval_percentage]
})

# Set seaborn style
colors = sns.color_palette("pastel")

# Create pie chart
plt.figure(figsize=(10, 7))
ax =plt.subplot()
plt.pie(data1['Percentage'], labels=data1['Status'], colors=colors, 
        autopct='%1.1f%%', startangle=90, explode=[0.1, 0])
plt.title('Percentage of Approved Applications for Self Employed Applicants', fontsize=16)

# save the plot
plt.savefig('visualizations/5.1_self_employed_approval_percentage.png')
print("Plot saved as 'visualizations/5.1_self_employed_approval_percentage.png'")
plt.close()

# -----5.2-----

# Calculate percentage of rejected applications for married male applicants
married_male = loan_df.filter((col('Married') == 'Yes') & (col('Gender') == 'Male'))
total_married_male = married_male.count()
rejected_married_male = married_male.filter(col('Application_Status') == 'N').count()
rejection_percentage = (rejected_married_male / total_married_male) * 100

data2 = pd.DataFrame({
    'Status': ['Rejected', 'Approved'],
    'Percentage': [rejection_percentage, 100 - rejection_percentage]
})


# Create pie chart
plt.figure(figsize=(10, 7))
ax =plt.subplot()
plt.pie(data2['Percentage'], labels=data2['Status'], colors=colors, 
        autopct='%1.1f%%', startangle=90, explode=[0.1, 0])
plt.title('Percentage of Rejected Applications for Married Male Applicants', fontsize=16)

# save the plot
plt.savefig('visualizations/5.2_married_male_rejection_percentage.png')
print("Plot saved as 'visualizations/5.2_married_male_rejection_percentage.png'")
plt.close()

# -----5.3-----

cc_df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:mysql://localhost:3306/creditcard_capstone') \
    .option('dbtable', 'CDW_SAPP_CREDIT_CARD') \
    .option('user', 'root') \
    .option('password', 'password') \
    .load()
    
monthly_volume = cc_df.withColumn('Year', substring('TIME_ID', 1, 4)) \
    .withColumn('Month', substring('TIME_ID', 5, 2)) \
    .groupBy('Year', 'Month') \
    .agg(count('*').alias('Transaction_Count')) \
    .orderBy(desc('Transaction_Count')) \
    .limit(3)

# Collect data for plotting
top_months = monthly_volume.collect()


# Prepare data for Seaborn
data3 = pd.DataFrame(top_months, columns=['Year', 'Month', 'Transaction_Count'])
data3['Month'] = data3.apply(lambda row: f"{row['Month']}-{row['Year']}", axis=1)

# Set seaborn style
sns.set_palette('deep')

# Create bar chart

plt.figure(figsize=(10, 6))
ax = sns.barplot(x='Month', y='Transaction_Count', data=data3)

plt.title('Top Three Months with Largest Transaction Volume', fontsize=16)
plt.xlabel('Month')
plt.ylabel('Number of Transactions')

# Add value labels on top of bars
for i, v in enumerate(data3['Transaction_Count']):
    ax.text(i, v + 100, str(v), ha='center', va='bottom')
    

plt.tight_layout()

plt.savefig('visualizations/5.3_top_3_months_transaction_volume.png')
print("Plot saved as 'visualizations/5.3_top_3_months_transaction_volume.png'")
plt.close()

# -----5.4-----

branch_df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:mysql://localhost:3306/creditcard_capstone') \
    .option('dbtable', 'CDW_SAPP_BRANCH') \
    .option('user', 'root') \
    .option('password', 'password') \
    .load()


# # Check schema to ensure correct column types
# cc_df.printSchema()
# branch_df.printSchema()

# # Verify that the join keys are correct
# cc_df.select('BRANCH_CODE').distinct().show()
# branch_df.select('BRANCH_CODE').distinct().show()

# Perform the join, filter, group by, and aggregation
healthcare_transactions = cc_df.alias('cc') \
    .join(branch_df.alias('br'), col('cc.BRANCH_CODE') == col('br.BRANCH_CODE')) \
    .filter(col('cc.TRANSACTION_TYPE') == 'Healthcare') \
    .groupBy('br.BRANCH_CODE', 'br.BRANCH_NAME', 'br.BRANCH_STATE', 'br.BRANCH_ZIP') \
    .agg(sum('cc.TRANSACTION_VALUE').alias('TOTAL_VALUE')) \
    .orderBy(desc('TOTAL_VALUE')) \
    .limit(10)  # Get top 10 branches

# Create a more meaningful branch identifier instead of "Example Bank"
healthcare_transactions = healthcare_transactions.withColumn(
    'BRANCH_IDENTIFIER', 
    concat(col('BRANCH_CODE'), lit(' - '), col('BRANCH_STATE'), lit(' '), col('BRANCH_ZIP'))
)

# Collect data and create pandas DataFrame with correct column names
column_names = ['BRANCH_CODE', 'BRANCH_NAME', 'BRANCH_STATE', 'BRANCH_ZIP', 'TOTAL_VALUE', 'BRANCH_IDENTIFIER']
data4 = pd.DataFrame(healthcare_transactions.collect(), columns=column_names)


plt.figure(figsize=(14, 8))
ax = sns.barplot(x='BRANCH_IDENTIFIER', y='TOTAL_VALUE', data=data4)
plt.title('Top 10 Branches by Total Dollar Value of Healthcare Transactions', fontsize=16)
plt.xlabel('Branch (Code - State ZIP)', fontsize=12)
plt.ylabel('Total Transaction Value ($)', fontsize=12)

for i, v in enumerate(data4['TOTAL_VALUE']):
    ax.text(i, v + 100, f'${v:,.0f}', ha='center', va='bottom', fontsize=10)

# Rotate x-axis labels for better readability
plt.xticks(rotation=45, ha='right')

# Adjust layout to prevent cutting off labels
plt.tight_layout()

# Save the plot
plt.savefig('visualizations/5.4_top_branches_healthcare_transactions.png')
print("Plot saved as 'visualizations/5.4_top_branches_healthcare_transactions.png'")
plt.close()


spark.stop()