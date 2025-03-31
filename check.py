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












# import psycopg2
# from sqlalchemy import create_engine

# # Connection parameters
# db_params = {
#     'host': 'database-1.c16qo4am8j9a.eu-north-1.rds.amazonaws.com',
#     'user': 'postgres',
#     'password': 'kjsdlggl203KDHflw0',
#     'port': '5432',
#     'dbname': 'postgres'  # Connect to default DB to drop others
# }

# # Database to delete/recreate
# target_db = 'coin_data_structured'

# try:
#     # Connect to PostgreSQL (using psycopg2 as SQLAlchemy can't drop DBs in active connection)
#     conn = psycopg2.connect(**db_params)
#     conn.autocommit = True  # Required for DB operations
#     cursor = conn.cursor()
    
#     # Terminate existing connections
#     cursor.execute(f"""
#         SELECT pg_terminate_backend(pg_stat_activity.pid)
#         FROM pg_stat_activity
#         WHERE pg_stat_activity.datname = '{target_db}'
#     """)
    
#     # Drop database
#     cursor.execute(f"DROP DATABASE IF EXISTS {target_db}")
#     print(f"Database {target_db} dropped successfully")
    
#     # # Create new database
#     # cursor.execute(f"CREATE DATABASE {target_db}")
#     # print(f"Database {target_db} created successfully")
    
# except Exception as e:
#     print(f"Error: {e}")
# finally:
#     if 'conn' in locals():
#         conn.close()















# import pandas as pd
# from sqlalchemy import create_engine, text

# # Database connection details (same as before)
# db_host = 'database-1.c16qo4am8j9a.eu-north-1.rds.amazonaws.com'
# db_port = '5432'
# db_name = 'postgres'  # or your specific database name
# db_user = 'postgres'
# db_password = 'kjsdlggl203KDHflw0'
# table_name = 'coin_data_structured'

# # Create engine with SSL
# engine = create_engine(
#     f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}',
#     connect_args={'sslmode': 'require'}
# )

# try:
#     with engine.connect() as conn:
#         # Drop the table if it exists
#         conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
#         conn.commit()
#         print(f"Table '{table_name}' dropped successfully")
        
#         # Verify the table is gone
#         result = conn.execute(text(f"""
#             SELECT EXISTS (
#                 SELECT FROM information_schema.tables 
#                 WHERE table_name = '{table_name.lower()}'
#             );
#         """))
#         exists = result.scalar()
#         print(f"Table exists after drop? {exists}")
        
# except Exception as e:
#     print(f"Error: {e}")
# finally:
#     engine.dispose()