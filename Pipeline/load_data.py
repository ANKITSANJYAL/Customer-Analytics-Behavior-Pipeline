from prefect import flow, task
import pandas as pd
import mysql.connector
from datetime import timedelta
from prefect.server.schemas.schedules import IntervalSchedule

@task
def load_data():
    # Load the preprocessed data
    df = pd.read_csv('Data/preprocessed.csv')

    # Connect to MySQL
    try:
        conn = mysql.connector.connect(
            unix_socket='/tmp/mysql.sock',  # Default socket path on macOS
            user='root',
            password='Alb3rt@123456789',
            database='retail'
        )
        cursor = conn.cursor()

        # Insert data into Customers table (skip duplicates)
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT IGNORE INTO Customers (CustomerID, Country)
                VALUES (%s, %s)
            """, (row['CustomerID'], row['Country']))

        # Insert data into Transactions table
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO Transactions (CustomerID, Description, Quantity, InvoiceDate, UnitPrice, Revenue)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (row['CustomerID'], row['Description'], row['Quantity'], row['InvoiceDate'], row['UnitPrice'], row['Revenue']))

        # Insert or update data in Metrics table
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO Metrics (CustomerID, CLV, AOV, Churned)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE CLV = VALUES(CLV), AOV = VALUES(AOV), Churned = VALUES(Churned);
            """, (row['CustomerID'], row['CLV'], row['AOV'], row['Churned']))

        # Commit the changes
        conn.commit()
        print("Data loaded into MySQL successfully!")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Close the connection
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

@flow(log_prints=True)
def pipeline():
    load_data()

if __name__ == "__main__":
    # Create a daily schedule
    schedule = IntervalSchedule(interval=timedelta(days=1))

    # Deploy the flow
    pipeline.deploy(
        name="Daily Pipeline",
        schedule=schedule,
        work_pool_name="default-agent-pool",  # Use the default work pool
        push=False  # Set to True if you're using a remote storage (e.g., Docker, GitHub)
    )