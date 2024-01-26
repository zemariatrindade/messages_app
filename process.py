import sqlite3
import pandas as pd
import spacy
from datetime import datetime
nlp = spacy.load("en_core_web_sm")


# Create table proc_log if it does not exist
def create_proc_log_table():
    # connecting to the sup-san-reviews.db. If not existent, creates an empty db
    conn = sqlite3.connect('sup-san-reviews.db')
    c = conn.cursor()

    # IF NOT EXISTS = only creates a table, if the proc_log table doesn't exist
    c.execute(
        "CREATE TABLE IF NOT EXISTS proc_log(uuid TEXT, proc_time DATE)")

    # Closing the connection
    c.close()
    conn.close()


# Create table proc_messages if it does not exist
def create_proc_messages_table():
    # connecting to the sup-san-reviews.db. If not existent, creates an empty db
    conn = sqlite3.connect('sup-san-reviews.db')
    c = conn.cursor()

    # IF NOT EXISTS = only creates a table, if the proc_messages table doesn't exist
    c.execute(
        "CREATE TABLE IF NOT EXISTS proc_messages(timestamp DATE, uuid TEXT, message TEXT, category TEXT,\
        num_lemm INTEGER, num_char INTEGER )")

    # Closing the connection
    c.close()
    conn.close()


# insert in the control table the uuids that are in the raw messages table but not in the control table
def query_insert_not_in_log():
    conn = sqlite3.connect('sup-san-reviews.db')
    c = conn.cursor()

    sql_query = ('INSERT INTO proc_log(uuid, proc_time)\
     SELECT raw_messages.uuid, NULL\
     FROM raw_messages\
     WHERE NOT EXISTS( SELECT 1 FROM proc_log WHERE raw_messages.uuid = proc_log.uuid);')

    c.execute(sql_query)
    conn.commit()

    c.close()
    conn.close()


# Get df with records that exist in the control table but have an empty time field
def query_empty_in_log():
    conn = sqlite3.connect('sup-san-reviews.db')
    c = conn.cursor()

    sql_query = "SELECT timestamp, proc_log.uuid, message\
    FROM proc_log\
    LEFT JOIN raw_messages on proc_log.uuid = raw_messages.uuid\
    WHERE proc_log.proc_time is NULL"

    messages_df = pd.read_sql_query(sql_query, conn)
    
    c.close()
    conn.close()

    return messages_df


# Add new columns (category, n_lemm, n_char) to the df to add to proc_messages table
def proc_message(messages_df):
    food_lemmas = ["sandwich", "bread", "meat", "cheese", "ham", "omelette", "food", "meal"]
    service_lemmas = ["waiter", "service", "table"]

    category_list = []
    n_lemmas = []
    n_characters = []

    for message in messages_df["message"]:
        food_score = 0
        service_score = 0

        doc = nlp(message)  # tokenizing
        lemma_tokens = [token.lemma_ for token in doc]  # lemmazation

        n_lemmas.append(len(lemma_tokens))
        n_characters.append(len(message))

        for lemma in lemma_tokens:  # 1st score board
            if lemma in food_lemmas:
                food_score += 1
            elif lemma in service_lemmas:
                service_score += 1

        if (food_score == 0) & (service_score == 0):  # If there are NO food or service lemmas
            category_list.append("GENERAL")  # Assigning category

        else:  # if there ARE food or service lemmas
            for ent in doc.ents:
                if ent.label_ == "MONEY":
                    service_score += 1

            if food_score >= service_score:  # Assigning category
                category_list.append("FOOD")
            elif food_score < service_score:
                category_list.append("SERVICE")

    messages_df["category"] = category_list
    messages_df["num_lemm"] = n_lemmas
    messages_df["num_char"] = n_characters

    return messages_df


# Append the df with new columns to proc_messages table (df to ddb)
def append_proc_messages(messages_df):
    # connecting to the db
    conn = sqlite3.connect('sup-san-reviews.db')
    c = conn.cursor()

    # Appends messages to the raw_messages table
    messages_df.to_sql("proc_messages", conn, if_exists="append", index=False)

    # closing connection
    c.close()
    conn.close()


# Update the NULL records of the column proc_time in proc_loc table with current timestamp
def update_proc_time():
    now = datetime.now()
    date_string = now.strftime("%d/%m/%Y %H:%M:%S")

    conn = sqlite3.connect('sup-san-reviews.db')
    c = conn.cursor()

    sql_query = 'UPDATE proc_log SET proc_time = ? WHERE proc_time IS NULL'

    c.execute(sql_query, (date_string,))
    conn.commit()

    c.close()
    conn.close()


if __name__ == '__main__':

    # Create proc_log table in db if not existent
    create_proc_log_table()

    # Insert in the control table the uuids that are in the raw messages table but not in the control table
    query_insert_not_in_log()

    # Read all messages that exist in the control table but have an empty time field
    messages_to_process = query_empty_in_log()

    # Process messages and calculate new fields
    df = proc_message(messages_to_process)

    # Create proc_messages table in db if not existent
    create_proc_messages_table()

    # Insert them in the processed table proc_messages
    append_proc_messages(df)

    # Update the proc_time field in the proc_log table
    update_proc_time()
