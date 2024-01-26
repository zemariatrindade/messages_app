import sys
import sqlite3
import pandas as pd
import json


def query_final_messages():
    conn = sqlite3.connect('sup-san-reviews.db')
    c = conn.cursor()

    sql_query = "SELECT * FROM proc_messages WHERE timestamp >= ?"

    messages_df = pd.read_sql_query(sql_query, conn, params=(filter_1,))

    c.close()
    conn.close()

    return messages_df


def create_json_file(messages_df):
    num = len(messages_df)
    list_of_dicts = messages_df.to_dict(orient="records")

    # Create a dictionary with the desired structure
    json_data = {
        "num": num,
        "messages": list_of_dicts
    }

    # Write the dictionary to a JSON file
    with open('messages.json', 'w') as json_file:
        json.dump(json_data, json_file, indent=2)


if __name__ == '__main__':
    # Accessing the csv file from the parameter inputted in the terminal
    filter_1 = sys.argv[1]

    # Convert the string to a datetime object
    # date_filter = datetime.strptime(filter, date_format)
    
    # Query messages with timestamp higher or equal to input parameter
    messages = query_final_messages()

    # Create json file
    create_json_file(messages)
