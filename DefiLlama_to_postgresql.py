import os
import pandas as pd
from sqlalchemy import create_engine
import sys
from dotenv import load_dotenv

# Load data from CSV
csv_file = 'output_data.csv'
data = pd.read_csv(csv_file)

# Load environment variables from .env file
load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
table_name = 'coin_data_structured'

# Create a connection to PostgreSQL
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Write data to PostgreSQL
try:
    data.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data successfully written to table '{table_name}' in PostgreSQL.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    engine.dispose()

