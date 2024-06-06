import psycopg2
import smtplib
import ast
import logging
import os
import requests
import json
import pandas as pd
from psycopg2 import DatabaseError
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
from datetime import datetime

DBNAME_REDSHIFT = os.getenv('DBNAME_REDSHIFT')
HOST_REDSHIFT = os.getenv('HOST_REDSHIFT')
PASS_REDSHIFT = os.getenv('PASS_REDSHIFT')
USER_REDSHIFT = os.getenv('USER_REDSHIFT')
SCHEMA_NAME_REDSHIFT = os.getenv('SCHEMA_NAME_REDSHIFT')
TABLE_NAME_REDSHIFT = os.getenv('TABLE_NAME_REDSHIFT')
PORT_REDSHIFT = os.getenv('PORT_REDSHIFT')
DS_DATE = os.getenv('DS_DATE')
DS_TOMORROW = os.getenv('DS_TOMORROW')
EMAIL_USERNAME = os.getenv('EMAIL_USERNAME')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
EMAIL_LIST_TO_ALERT = ast.literal_eval(os.getenv('EMAIL_LIST_TO_ALERT'))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Libraries calls ok")
CUR_TIME = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.info(f"Start Time : {CUR_TIME} ")
logging.info(f'type(EMAIL_LIST_TO_ALERT): {type(EMAIL_LIST_TO_ALERT)}')
logging.info(f'EMAIL_LIST_TO_ALERT: {EMAIL_LIST_TO_ALERT}')

INSERT_QUERY = f"""
        INSERT INTO {SCHEMA_NAME_REDSHIFT}.{TABLE_NAME_REDSHIFT} (
            symbol, priceChange, priceChangePercent, weightedAvgPrice, prevClosePrice, lastPrice, lastQty,
            bidPrice, bidQty, askPrice, askQty, openPrice, highPrice, lowPrice, volume, quoteVolume,
            openTime, closeTime, firstId, lastId, count, created_at
        ) VALUES %s;
"""

CREATE_TABLE = f"""
CREATE TABLE {USER_REDSHIFT}.{TABLE_NAME_REDSHIFT} (
    symbol VARCHAR(256),
    priceChange FLOAT,
    priceChangePercent FLOAT,
    weightedAvgPrice FLOAT,
    prevClosePrice FLOAT,
    lastPrice FLOAT,
    lastQty FLOAT,
    bidPrice FLOAT,
    bidQty FLOAT,
    askPrice FLOAT,
    askQty FLOAT,
    openPrice FLOAT,
    highPrice FLOAT,
    lowPrice FLOAT,
    volume FLOAT,
    quoteVolume FLOAT,
    openTime TIMESTAMP,
    closeTime TIMESTAMP,
    firstId BIGINT,
    lastId BIGINT,
    count BIGINT,
    created_at TIMESTAMP
);
"""


def connect_to_redshift(host_redshift, dbname_redshift, user_redshift, pass_redshift, port_redshift=5439):
    """
    Establishes a connection to a Redshift database using the provided connection parameters.

    Parameters:
    host_redshift (str): The hostname or IP address of the Redshift server.
    dbname_redshift (str): The name of the database to connect to.
    user_redshift (str): The username used to authenticate with Redshift.
    pass_redshift (str): The password used to authenticate with Redshift.
    port_redshift (str or int): The port number on which the Redshift server is listening. Default is 5439.

    Returns:
    Connection: A psycopg2 connection object that can be used to interact with the database.

    Raises:
    OperationalError: An error from the database if the connection fails, which is caught and logged.
    """
    logging.info("Connecting to Redshift...")
    try:
        conn = psycopg2.connect(
            dbname=dbname_redshift,
            user=user_redshift,
            password=pass_redshift,
            host=host_redshift,
            port=port_redshift
        )
        logging.info("Connection established")
        return conn
    except OperationalError as e:
        logging.error(f"Failed to connect to Redshift: {e}")
        raise


def execute_query(conn, query):
    """
    Executes a SQL query using the provided database connection and commits the changes.

    Parameters:
    conn (Connection): A psycopg2 connection object representing the connection to the database.
    query (str): The SQL query to be executed.

    Returns:
    None: The function returns None, indicating the query was executed and committed.

    Raises:
    DatabaseError: If an error occurs during the query execution. The error is caught, logged, and re-raised to the caller.
    """
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        logging.info("Query executed and committed successfully.")
    except DatabaseError as error:
        conn.rollback()
        logging.error(f"Failed to execute query: {error}")
        raise
    finally:
        if cursor:
            cursor.close()


def check_table_if_exists(conn, schema_name, table_name):
    """
    Checks if a specified table exists within a given schema in the database using a database connection.

    Parameters:
    conn (Connection): The database connection from which to create a cursor.
    schema_name (str): The name of the schema in the database where the table is located.
    table_name (str): The name of the table to check for existence.

    Returns:
    bool: True if the table exists, False otherwise.

    Raises:
    DatabaseError: An error from the database if the query execution fails, should be caught and handled by the caller.
    """
    logging.info("Checking if table exists...")
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables WHERE
                table_schema = '{schema_name}' AND table_name = '{table_name}'
            )
        """)
        table_exists = cur.fetchone()[0]
        return table_exists
    except DatabaseError as e:
        logging.error(f"Error checking if table exists: {e}")
        raise
    finally:
        cur.close()


def truncate_table_if_exists(schema_name_redshift, table_name_redshift):
    """
    Checks if a specified table exists within the Redshift database and truncates it if it does.

    Parameters:
    schema_name_redshift (str): The schema name in the Redshift database where the table resides.
    table_name_redshift (str): The table name to check and potentially truncate.

    Returns:
    None

    Raises:
    Exception: Any exception raised during database operations is caught, logged, and re-raised.
    """
    conn = None
    try:
        conn = connect_to_redshift(
            HOST_REDSHIFT, DBNAME_REDSHIFT, USER_REDSHIFT, PASS_REDSHIFT, PORT_REDSHIFT)
        table_exists = check_table_if_exists(
            conn, schema_name_redshift, table_name_redshift)
        if table_exists:
            logging.info(
                f"Truncating table {schema_name_redshift}.{table_name_redshift}")
            query = f"truncate table {schema_name_redshift}.{table_name_redshift}"
            execute_query(conn, query)
        else:
            logging.info(
                f"Table {schema_name_redshift}.{table_name_redshift} does not exist.")
    except Exception as e:
        logging.error(
            f"An error occurred while attempting to truncate table: {e}")
        raise
    finally:
        if conn:
            conn.close()


def create_table_if_not_exists(schema_name_redshift, table_name_redshift):
    """
    Connects to a Redshift database and creates a table if it does not already exist in the specified schema.

    Parameters:
    schema_name_redshift (str): The schema name in the Redshift database where the table should be located.
    table_name_redshift (str): The name of the table to be created if it does not exist.

    Globals:
    - HOST_REDSHIFT, DBNAME_REDSHIFT, USER_REDSHIFT, PASS_REDSHIFT, PORT_REDSHIFT: Database connection parameters.
    - CREATE_TABLE (str): SQL command string to create the table, assumed to be globally defined.

    Returns:
    None: This function does not return any value.

    Raises:
    Exception: Captures any exceptions related to database connections or SQL execution, logs them, and re-raises.
    """
    conn = None
    cur = None
    try:
        conn = connect_to_redshift(
            HOST_REDSHIFT, DBNAME_REDSHIFT, USER_REDSHIFT, PASS_REDSHIFT, PORT_REDSHIFT)
        cur = conn.cursor()
        table_exists = check_table_if_exists(
            conn, schema_name_redshift, table_name_redshift)
        logging.info(f"Table exists: {table_exists}")
        if not table_exists:
            logging.info(
                f"Creating table {schema_name_redshift}.{table_name_redshift}")
            cur.execute(CREATE_TABLE)
            conn.commit()
        else:
            logging.info(
                f"Table {schema_name_redshift}.{table_name_redshift} already exists.")
    except Exception as e:
        logging.error(f"An error occurred while creating table: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def insert_data_into_redshift(conn, df, insert_query):
    """
    Inserts data into a Redshift table.

    Parameters:
        conn (psycopg2.connection): The connection object for the Redshift database.
        data (list of tuples): The data to be inserted into the table. Each tuple represents a row.

    Returns:
        None
    """
    cur = None
    try:
        data_to_insert = [tuple(row) for row in df.values]
        cur = conn.cursor()
        execute_values(
            cur,
            insert_query,
            data_to_insert,
            page_size=len(data_to_insert)
        )
        conn.commit()
        logging.info("Data inserted into Redshift successfully.")
    except OperationalError as e:
        logging.error(f"Operational Error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        if cur is not None:
            cur.close()
            logging.info("Cursor closed.")
        if conn is not None:
            conn.close()
            logging.info("Database connection closed.")


def clean_data_and_drop_duplicates_if_exits(df):
    """
    Cleans the DataFrame containing data received from an API and removes duplicates based on the 'symbol' column,
    keeping only the last entry. This function also standardizes the format of date columns and converts all data
    types according to expected schema before insertion into a Redshift database.

    Parameters:
        df (pandas.DataFrame): DataFrame containing the data received from the API.

    Returns:
        pandas.DataFrame: Cleaned DataFrame ready for insertion into Redshift table.
    """
    df['openTime'] = pd.to_datetime(df['openTime'], unit='ms')
    df['closeTime'] = pd.to_datetime(df['closeTime'], unit='ms')
    df['created_at'] = pd.to_datetime(df['created_at'])

    expected_data_types = {
        'symbol': str, 'priceChange': float, 'priceChangePercent': float, 'weightedAvgPrice': float,
        'prevClosePrice': float, 'lastPrice': float, 'lastQty': float, 'bidPrice': float, 'bidQty': float,
        'askPrice': float, 'askQty': float, 'openPrice': float, 'highPrice': float, 'lowPrice': float,
        'volume': float, 'quoteVolume': float, 'openTime': str, 'closeTime': str,
        'firstId': int, 'lastId': int, 'count': int, 'created_at': str
    }

    for column, dtype in expected_data_types.items():
        if column in df.columns:
            if dtype == str:
                df[column] = df[column].astype(str)
            else:
                df[column] = df[column].astype(dtype)

    important_columns = ['symbol', 'priceChange', 'priceChangePercent', 'weightedAvgPrice', 'prevClosePrice',
                         'lastPrice', 'lastQty', 'bidPrice', 'bidQty', 'askPrice', 'askQty', 'openPrice',
                         'highPrice', 'lowPrice', 'volume', 'quoteVolume', 'openTime', 'closeTime',
                         'firstId', 'lastId', 'count', 'created_at']
    df.dropna(subset=important_columns, inplace=True)
    df.drop_duplicates(subset='symbol', keep='last', inplace=True)

    return df


def send_email(subject, body):
    """
    Send an email with the specified message and date.

    Args:
    - mes (str): The month of the appointment.
    - dia (str): The day of the appointment.
    """

    sent_from = EMAIL_USERNAME
    subject = f'{subject}'
    body = f'{body}'

    to = EMAIL_LIST_TO_ALERT

    email_text = "From: %s\nTo: %s\nSubject: %s\n\n%s" % (
        sent_from, ", ".join(to), subject, body)

    print(email_text)

    try:
        print("Sending Email...")
        server = smtplib.SMTP('smtp.dreamhost.com', 587)
        server.starttls()
        server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        server.sendmail(sent_from, to, email_text)
        server.close()
        print('Email sent!')
    except Exception as e:
        print("Error: ", e)


def read_json_to_dataframe(file_path):
    """
    Reads a JSON file containing cryptocurrency prices and converts it to a pandas DataFrame.

    Args:
    - file_path (str): The path to the JSON file.

    Returns:
    - pd.DataFrame: A DataFrame containing the coin, min price, and max price.

    Raises:
    - FileNotFoundError: If the specified file does not exist.
    - ValueError: If the JSON file contains invalid data.
    - Exception: For any other exceptions that may occur.
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)

        df = pd.DataFrame(data).T.reset_index()
        df.columns = ['coin_pair', 'min_price', 'max_price']
        return df

    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
    except ValueError as e:
        print(f"Error: Could not parse JSON. {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def check_price_alerts(df, result_df):
    """
    Checks for price alerts for each coin pair in the dataframe and returns a single text with all results.
    Returns None if there are no alerts.

    Args:
    - df (pd.DataFrame): DataFrame with columns ['coin_pair', 'min_price', 'max_price']
    - result_df (pd.DataFrame): DataFrame with columns ['symbol', 'lastPrice']

    Returns:
    - str or None: A string with all alerts or None if no alerts are found
    """
    try:
        alerts = []

        for index, row in df.iterrows():
            coin_pair = row['coin_pair']
            min_price = row['min_price']
            max_price = row['max_price']

            if coin_pair in result_df['symbol'].values:
                last_price = result_df.loc[result_df['symbol']
                                           == coin_pair, 'lastPrice'].values[0]

                if last_price > max_price:
                    alerts.append(
                        f"Alert: The price of {coin_pair} ({last_price}) is greater than the alert max price ({max_price}).")
                elif last_price < min_price:
                    alerts.append(
                        f"Alert: The price of {coin_pair} ({last_price}) is lower than the alert min price ({min_price}).")

        if alerts:
            return '\n'.join(alerts)
        else:
            return None

    except KeyError as e:
        return f"Key error: {e}"
    except Exception as e:
        return f"An unexpected error occurred: {e}"


def main():
    try:
        logging.info("Making requests to the Binance API.")
        r = requests.get('https://api.binance.com/api/v1/ticker/24hr')

        if r.status_code == 200:
            logging.info(
                f"Connection successful; Requests Status: {r.status_code}")
            result = r.json()
            result_df = pd.json_normalize(result)

            logging.info(
                "Adding a column named 'created_at' with the DS_DATE.")
            result_df['created_at'] = DS_DATE

            logging.info("Cleaning data form dataframe...")
            result_df = clean_data_and_drop_duplicates_if_exits(result_df)

            conn = connect_to_redshift(
                HOST_REDSHIFT, DBNAME_REDSHIFT, USER_REDSHIFT, PASS_REDSHIFT, PORT_REDSHIFT)

            query = f"""
                DELETE FROM {SCHEMA_NAME_REDSHIFT}.{TABLE_NAME_REDSHIFT}
                WHERE created_at >= '{DS_DATE}'::timestamp and
                created_at < '{DS_TOMORROW}'::timestamp
            """
            logging.info(f"{query}")
            execute_query(conn, query)

            logging.info("Loading data from Binance to Redshift.")

            create_table_if_not_exists(
                SCHEMA_NAME_REDSHIFT, TABLE_NAME_REDSHIFT)
            insert_data_into_redshift(conn, result_df, INSERT_QUERY)
            logging.info("Reading json alerts...")
            df = read_json_to_dataframe('/opt/airflow/alerts/alerts.json')
            alerts = check_price_alerts(df, result_df)
            logging.info(alerts)

            logging.info("Sending alerts...")
            send_email("There exists alerts.", f"{alerts}")

        else:
            logging.info(
                f"Unable to connect to Binance; Requests Status: {r.status_code}")

    except OperationalError as e:
        logging.error(f"Operational Error: {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


if __name__ == '__main__':
    main()
    CUR_TIME = datetime.now().strftime("%Y%m%d_%H%M%S")
    logging.info(f"End Time : {CUR_TIME} ")
    logging.info('PROCESS_EXECUTED_SUCCESSFULLY')
