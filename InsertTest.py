import requests
import json
import psycopg2
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Define the PostgreSQL connection parameters
dbname = 'Stock_Data'
user = 'postgres'
password = 'youngest97'
host = 'localhost'
port = '5432'

# Function to fetch and insert data into the database
def fetch_and_insert_data(url, symbol, conn):
    try:
        cursor = conn.cursor()

        # Fetch data from the Alpha Vantage API
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()

            # Parse the JSON data and insert into the database
            for timestamp, values in data['Time Series (5min)'].items():
                open_price = float(values['1. open'])
                high_price = float(values['2. high'])
                low_price = float(values['3. low'])
                close_price = float(values['4. close'])
                volume = int(values['5. volume'])

                # Insert data into the stock_data table
                cursor.execute("""
                    INSERT INTO stock_data (Symbol, Timestamp, Open_Price, High_Price, Low_Price, Close_Price, Volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (symbol, timestamp, open_price, high_price, low_price, close_price, volume))

            # Commit the changes after processing each month's data
            conn.commit()
            print(f"Data inserted successfully for {symbol} - {timestamp}")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error inserting data for {symbol}: {e}")

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(
        dbname=dbname, user=user, password=password, host=host, port=port
    )

    # Define the symbol and initial date for fetching data
    symbol = 'SPY'
    start_date = datetime(2023, 12, 1)  # December 2023
    end_date = datetime(2015, 1, 1)    # January 2015

    # Loop through the months and fetch data for each month
    while start_date > end_date:
        month_str = start_date.strftime('%Y-%m')
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&month={month_str}&outputsize=full&apikey=OYWU5ENAOLBW52DH'

        # Fetch and insert data for the current month
        fetch_and_insert_data(url, symbol, conn)

        # Move to the previous month using relativedelta
        start_date -= relativedelta(months=1)

    # Close the database connection
    conn.close()
    print("Database connection closed")

except psycopg2.Error as e:
    print(f"Error connecting to PostgreSQL: {e}")
