import yfinance as yf
from datetime import datetime, timedelta

# Define the stock symbol (e.g., "SPY" for the S&P 500 ETF) and the desired date range
stock_symbol = "SPY"
start_date = datetime(2023, 12, 20)  # Start date: December 20, 2023
end_date = start_date + timedelta(days=2)  # End date: December 21, 2023 (2 days in total)

# Get stock data for the desired date range
stock_data = yf.download(tickers=stock_symbol, start=start_date, end=end_date, interval="5m")

# Display opening, closing, high, low prices, and volume for each 5-minute interval within the date range
for idx, interval_data in stock_data.iterrows():
    print(f"Time: {idx.time()} - Opening Price: {interval_data['Open']} - Closing Price: {interval_data['Close']} - High Price: {interval_data['High']} - Low Price: {interval_data['Low']} - Volume: {interval_data['Volume']}")
