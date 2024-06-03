import json
from utils import get_db_connection
from utils import fetch_data
from utils import insert_data

def main():
    # Load configuration from config.json
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    url = config['url']
    data = fetch_data(url)
    if data:
        conn = get_db_connection()
        try:
            insert_data(conn, data)
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    main()
