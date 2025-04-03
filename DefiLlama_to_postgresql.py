import os
import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, Float, MetaData
from sqlalchemy.dialects.postgresql import insert
import sys
import logging
from dotenv import load_dotenv
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def validate_env_variables():
    """Validate that all required environment variables are set"""
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    return True

def create_database_engine():
    """Create and return a database engine"""
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    
    try:
        engine = create_engine(connection_string)
        # Test connection
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        logger.info("Database connection established successfully")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None

def load_csv_data(csv_file):
    """Load data from CSV file"""
    try:
        if not os.path.exists(csv_file):
            logger.error(f"CSV file not found: {csv_file}")
            return None
        
        data = pd.read_csv(csv_file)
        logger.info(f"Loaded {len(data)} rows from {csv_file}")
        return data
    except Exception as e:
        logger.error(f"Error loading CSV data: {e}")
        return None

def setup_database_table(engine, table_name):
    """Create table if it doesn't exist"""
    metadata = MetaData()
    
    # Define table schema
    table = Table(
        table_name, metadata,
        Column('Name', String, primary_key=True),
        Column('Symbol', String),
        Column('TVL', Float),
        Column('Price (USD)', Float),
        Column('Market Cap (USD)', Float),
        Column('24h Volume (USD)', Float),
        Column('Circulating Supply', Float),
        Column('Last Updated', String),
        Column('timestamp', String, default=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )
    
    try:
        metadata.create_all(engine)
        logger.info(f"Table {table_name} created or already exists")
        return table
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return None

def upsert_data(engine, table, data_df, table_name):
    """Insert or update data in the database table"""
    try:
        # Clean data: replace 'N/A' with None
        for column in data_df.columns:
            data_df[column] = data_df[column].replace('N/A', None)
        
        # Add timestamp column if not exists
        if 'timestamp' not in data_df.columns:
            data_df['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Convert dataframe to list of dictionaries
        data_records = data_df.to_dict(orient='records')
        
        # Perform upsert operation
        with engine.begin() as conn:
            for record in data_records:
                # Create insert statement
                insert_stmt = insert(table).values(**record)
                
                # Create upsert statement
                upsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=['Name'],
                    set_={col: getattr(insert_stmt.excluded, col) for col in record if col != 'Name'}
                )
                
                # Execute statement
                conn.execute(upsert_stmt)
        
        logger.info(f"Successfully upserted {len(data_records)} records to table '{table_name}'")
        return True
    except Exception as e:
        logger.error(f"Error upserting data: {e}")
        return False

def main():
    """Main function to orchestrate data loading and database operations"""
    # Load environment variables
    load_dotenv()
    
    # Validate environment variables
    if not validate_env_variables():
        sys.exit(1)
    
    # CSV file path
    csv_file = 'output_data.csv'
    table_name = 'coin_data_structured'
    
    # Load data from CSV
    data = load_csv_data(csv_file)
    if data is None:
        sys.exit(1)
    
    # Create database engine
    engine = create_database_engine()
    if engine is None:
        sys.exit(1)
    
    try:
        # Setup database table
        table = setup_database_table(engine, table_name)
        if table is None:
            sys.exit(1)
        
        # Upsert data to database
        if upsert_data(engine, table, data, table_name):
            logger.info("Data upload completed successfully")
        else:
            logger.error("Data upload failed")
            sys.exit(1)
    finally:
        if engine:
            engine.dispose()
            logger.info("Database connection closed")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        sys.exit(1)

