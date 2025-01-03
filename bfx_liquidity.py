import requests
import sqlite3
import time
import os
from datetime import datetime

# Define the API URL
API_URL = "https://api-pub.bitfinex.com/v2/conf/pub:conf:node:ln:0"

# Sqlite database for storing the results
DATABASE = os.getenv("DATABASE_BFX")
if DATABASE is None:
    DATABASE = "bfx_data.db"

# Interval between the API calls
SLEEP_SECONDS = 600

# SQLite database setup
def setup_database():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS liquidity_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            local INTEGER,
            remote INTEGER
        )
        """
    )
    conn.commit()
    conn.close()

# Fetch data from the API
def fetch_data():
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()
    return data[0]['capacity']

# Get the last recorded data from the database
def get_last_record():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT local, remote FROM liquidity_history ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    conn.close()
    return result

# Insert new data into the database
def insert_data(local, remote):
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute(
        "INSERT INTO liquidity_history (timestamp, local, remote) VALUES (?, ?, ?)",
        (timestamp, local, remote)
    )
    conn.commit()
    conn.close()

# Main logic to fetch, compare, and store data
def main():
    setup_database()

    while True:
        try:
            data = fetch_data()
            local = data['local']
            remote = data['remote']

            last_record = get_last_record()

            if last_record is None or (local, remote) != last_record:
                insert_data(local, remote)
                print(f"New data inserted: local={local}, remote={remote}")

        except Exception as e:
            print(f"Error: {e}")

        time.sleep(SLEEP_SECONDS)  # Wait 10 minutes before fetching data again

if __name__ == "__main__":
    main()
