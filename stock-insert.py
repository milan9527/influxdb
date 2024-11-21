import random
import time
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# InfluxDB connection details
INFLUXDB_URL = 'https://xxxx.us-east-1.timestream-influxdb.amazonaws.com:8086'
INFLUXDB_TOKEN = 'xxxx'
INFLUXDB_ORG = 'aws'
INFLUXDB_BUCKET = 'stock_data'

# Create an InfluxDB client instance
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

# Get the write API
write_api = client.write_api(write_options=SYNCHRONOUS)

# Stock symbols
stock_symbols = ['AAPL', 'GOOG', 'MSFT', 'AMZN', 'FB']

# Initial stock prices
stock_prices = {symbol: round(random.uniform(1, 3000), 2) for symbol in stock_symbols}

while True:
    # Generate random stock tick data
    data = []
    for symbol in stock_symbols:
        open_price = stock_prices[symbol]
        price_range = random.uniform(-100, 100)  # Random price range between -100 and 100
        close_price = round(open_price + price_range, 2)
        close_price = max(1, min(close_price, 3000))  # Ensure price is within the range

        # Generate independent low and high prices
        low_price = round(min(open_price, close_price) - random.uniform(0, 50), 2)
        high_price = round(max(open_price, close_price) + random.uniform(0, 50), 2)

        volume = random.randint(1000, 10000)
        timestamp = datetime.utcnow()
        data.append(Point("stock_tick")
                    .tag("symbol", symbol)
                    .field("open", float(open_price))
                    .field("close", float(close_price))
                    .field("low", float(low_price))
                    .field("high", float(high_price))
                    .field("volume", volume)
                    .time(timestamp))

        # Update the stock price for the next iteration
        stock_prices[symbol] = close_price

    # Write the data to InfluxDB
    write_api.write(bucket=INFLUXDB_BUCKET, record=data)

    # Wait for 1 millisecond
    time.sleep(0.001)
