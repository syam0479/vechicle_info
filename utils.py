import psycopg2
from psycopg2 import sql
import json
import requests

import psycopg2
import json


def get_db_connection():
    # Load database configuration from config.json
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    # Connect to the PostgreSQL database using the credentials from the config file
    conn = psycopg2.connect(
        dbname=config['dbname'],
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port']
    )
    return conn


def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        try:
            data = response.json()
            return data
        except json.JSONDecodeError:
            print("Error decoding JSON")
            return None
    else:
        print(f"Failed to fetch data from URL. Status code: {response.status_code}")
        return None


def insert_data(conn, data):
    with conn.cursor() as cur:
        # Handle the Socrata JSON structure
        if isinstance(data, dict):
            # Look for the 'data' key that contains the actual rows
            if 'data' in data:
                rows = data['data']
                # Extract column names from 'meta' if it exists
                if 'meta' in data and 'view' in data['meta'] and 'columns' in data['meta']['view']:
                    column_info = data['meta']['view']['columns']
                    column_names = [col['name'] for col in column_info]
                else:
                    print("Could not find column names in JSON response")
                    return
            else:
                print("Could not find 'data' key in JSON response")
                return
        else:
            print("No data found in JSON or data is not in expected format")
            return

        print("Column Names:", column_names)

        # Generate CREATE TABLE statement
        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS EV ({})").format(
            sql.SQL(', ').join([
                sql.Identifier(col) + sql.SQL(' VARCHAR') for col in column_names
            ])
        )
        cur.execute(create_table_query)

        # Generate INSERT INTO statement
        insert_query = sql.SQL("INSERT INTO EV ({}) VALUES ({})").format(
            sql.SQL(', ').join(map(sql.Identifier, column_names)),
            sql.SQL(', ').join(sql.Placeholder() * len(column_names))
        )

        # Insert data into the table
        for row in rows:
            cur.execute(insert_query, row)

        # Commit changes
        conn.commit()