import pandas as pd
import os
from sqlalchemy import create_engine
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
table_name = 'coin_data_structured'

# Debugging: Print environment variables
print(f"DB_HOST: {db_host}, DB_PORT: {db_port}, DB_NAME: {db_name}, DB_USER: {db_user}, DB_PASSWORD: {'*' * len(db_password) if db_password else 'None'}")

# Ensure all environment variables are set
if not all([db_host, db_port, db_name, db_user, db_password]):
    print("Error: One or more environment variables are not set.")
    sys.exit(1)

# Create a connection to PostgreSQL
try:
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    print("Database connection established successfully.")
except Exception as e:
    print(f"Error creating database engine: {e}")
    sys.exit(1)

# Query the table
try:
    # Read data from the table into a Pandas DataFrame
    query = f"SELECT * FROM {table_name} LIMIT 10;"  # Limit to 10 rows for quick verification
    data = pd.read_sql(query, engine)
    
    # Print the data
    print(f"\nData from table '{table_name}':")
    print(data)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if 'engine' in locals():
        engine.dispose()








