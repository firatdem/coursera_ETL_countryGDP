# Code for ETL operations on Country-GDP data
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from io import StringIO
import sqlite3

'''
Run requirements and run this in console before compiling:

# wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv


'''


# Importing the required libraries

def log_progress(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_entry = f"{timestamp} : {message}\n"

    with open("code_log.txt", "a") as file:
        file.write(log_entry)


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    tables = soup.find_all("table", table_attribs)

    html_content = str(tables[0])
    df = pd.read_html(StringIO(html_content))[0]

    df['Market cap (US$ billion)'] = df['Market cap (US$ billion)'].astype(str)
    df['Market cap (US$ billion)'] = df['Market cap (US$ billion)'].str.replace(r'[^\d.]', '', regex=True).astype(float)

    print(df)  # This will print the DataFrame to the console
    log_progress("Data extracted (function flag)")

    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_df = pd.read_csv("exchange_rate.csv")
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['Market cap (US$ billion)']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['Market cap (US$ billion)']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['Market cap (US$ billion)']]

    print(df)  # This will print the DataFrame to the console
    log_progress("Data transformed (function flag)")

    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file (function flag)")


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, executing queries (function flag)")


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    result = cursor.fetchall()
    for row in result:
        print(row)
    log_progress("Process Complete (function flag)")


url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = {"class": "wikitable"}
csv_path = "exchange_rate.csv"
output_csv = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"

log_progress("Preliminaries complete. Initiating ETL process (main flag)")

# Task 2: Extract
df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process (main flag)")

# Task 3: Transform
df = transform(df, "exchange_rate.csv")
log_progress("Data transformation complete. Initiating Loading process (main flag)")

# Task 4: Load to CSV
load_to_csv(df, "largest_banks_transformed.csv")

# Task 5: Load to Database
sql_connection = sqlite3.connect("Banks.db")
log_progress("SQL Connection initiated (main flag)")
load_to_db(df, sql_connection, "Largest_banks")

# Task 6: Run Queries
run_query("SELECT * FROM Largest_banks", sql_connection)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", sql_connection)
run_query("SELECT `Bank name` FROM Largest_banks LIMIT 5", sql_connection)

# Close connection
sql_connection.close()
log_progress("Server Connection closed (main flag)")