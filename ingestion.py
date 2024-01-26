import sys
import pandas as pd
import sqlite3


# Function to upload the csv file with the messages information to a pandas df
def read_messages_csv(file):
    df = pd.read_csv(file, header=0)
    return df


# Function to create a raw_messages table in the sup-san-reviews.db
def create_raw_messages_table():
    # connecting to the sup-san-reviews.db. If not existent, creates an empty db
    conn = sqlite3.connect('sup-san-reviews.db')
    c = conn.cursor()

    # IF NOT EXISTS = only creates a table, if the raw_messages table doesn't exist
    c.execute(
        "CREATE TABLE IF NOT EXISTS raw_messages(timestamp DATE, uuid TEXT, message TEXT)")

    # Closing the connection
    c.close()
    conn.close()


# Function to append records to the raw_messages table in the sup-san-reviews.db
def data_entry(df):
    # connecting to the db
    conn = sqlite3.connect('sup-san-reviews.db')
    c = conn.cursor()

    # Appends messages to the raw_messages table
    df.to_sql("raw_messages", conn, if_exists="append", index=False)

    # closing connection
    c.close()
    conn.close()


if __name__ == '__main__':
    # Accessing the csv file from the parameter inputted in the terminal
    path_to_file = sys.argv[1]

    # Getting the pandas df from csv
    messages_df = read_messages_csv(path_to_file)

    # Creating the table if not exists in the sup-san-reviews.db
    create_raw_messages_table()

    # Adding the messages_df into the table of the db
    data_entry(messages_df)
