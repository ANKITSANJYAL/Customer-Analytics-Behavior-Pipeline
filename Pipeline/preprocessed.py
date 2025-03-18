import pandas as pd

# Load the dataset
df = pd.read_csv('Data/data.csv')

# Remove rows where CustomerID is missing
df = df.dropna(subset=['CustomerID'])

# Remove duplicates
df = df.drop_duplicates()

# Fill missing Description with 'Unknown'
df['Description'] = df['Description'].fillna('Unknown')

# Handle returns (negative Quantity)
df = df[df['Quantity'] > 0]

# Handle UnitPrice outliers (e.g., values of 0)
df = df[df['UnitPrice'] > 0]

# Convert InvoiceDate to datetime
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

# Convert CustomerID to int
df['CustomerID'] = df['CustomerID'].astype(int)

# Calculate revenue for each transaction
df['Revenue'] = df['Quantity'] * df['UnitPrice']

# Calculate Customer Lifetime Value (CLV)
clv = df.groupby('CustomerID')['Revenue'].sum().reset_index()
clv.columns = ['CustomerID', 'CLV']

# Calculate Average Order Value (AOV)
aov = df.groupby('CustomerID')['Revenue'].mean().reset_index()
aov.columns = ['CustomerID', 'AOV']

# Calculate Churn Status for Each Customer
last_purchase_date = df['InvoiceDate'].max()
six_months_ago = last_purchase_date - pd.DateOffset(months=6)

# Identify customers who haven't made a purchase in the last 6 months
churned_customers = df[df['InvoiceDate'] < six_months_ago]['CustomerID'].unique()

# Add Churn Status to the CLV DataFrame
clv['Churned'] = clv['CustomerID'].isin(churned_customers).astype(int)

# Merge CLV and AOV into the main DataFrame
df = df.merge(clv, on='CustomerID', how='left')
df = df.merge(aov, on='CustomerID', how='left')

# Save the preprocessed data to a CSV file
df.to_csv('Data/preprocessed.csv', index=False)

# Verify the preprocessed data
preprocessed_df = pd.read_csv('Data/preprocessed.csv')
print(preprocessed_df.head())
print(preprocessed_df.columns)