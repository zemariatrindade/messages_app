import pandas as pd
import random
from datetime import datetime, timedelta
import uuid


# Function to generate a unique UUID for each row
def generate_uuid():
    return str(uuid.uuid4())


# Function to create the CSV file with random timestamps and messages
def create_csv_file(file, rows, start, end):
    # Define the words to include in the 'message' column
    words = ['at', 'with', 'on', 'for', 'the', 'of', 'and', '.', 'sandwich', 'bread', 'meat', 'cheese', 'ham',
             'omelette', 'food', 'meal', 'waiter', 'service', 'table', 'gym', 'ketchup', 'do', 'you']

    # Generate data
    data = {
        'timestamp': [start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
                      for _ in range(rows)],
        'uuid': [generate_uuid() for _ in range(rows)],
    }

    # Create a DataFrame
    df = pd.DataFrame(data)

    # Write the DataFrame to a CSV file
    df.to_csv(file, index=False)
    print('CSV file created successfully.')
    print(df.info())


# Define the file path, the number of rows, and the date range for timestamps
file_path = 'restaurant_reviews.csv'
num_rows = 1000
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 20)

# Create the CSV file with random timestamps and messages
create_csv_file(file_path, num_rows, start_date, end_date)
