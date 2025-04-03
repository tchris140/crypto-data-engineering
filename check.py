import pandas as pd
import os
from sqlalchemy import create_engine, text
import sys
import logging
from tabulate import tabulate
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import MetaData, Table, select

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def validate_env_variables():
    """Validate that all required environment variables are set"""
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = []
    
    # Display environment variables (with masked password)
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
            logger.error(f"{var}: Not set")
        else:
            if var == 'DB_PASSWORD':
                logger.info(f"{var}: {'*' * len(value)}")
            else:
                logger.info(f"{var}: {value}")
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    return True

def create_database_engine():
    """Create and test database connection"""
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
            conn.execute(text("SELECT 1"))
        logger.info("Database connection established successfully")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None

def query_database_info(engine):
    """Query basic database information"""
    try:
        with engine.connect() as conn:
            # Check PostgreSQL version
            version_query = text("SELECT version();")
            version = conn.execute(version_query).scalar()
            logger.info(f"PostgreSQL version: {version}")
            
            # List all tables in the schema
            tables_query = text("""
                SELECT table_name, pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY pg_total_relation_size(quote_ident(table_name)) DESC;
            """)
            tables = conn.execute(tables_query).fetchall()
            
            if tables:
                logger.info("Tables in the database:")
                for table in tables:
                    logger.info(f"  - {table[0]} (Size: {table[1]})")
            else:
                logger.warning("No tables found in the database")
                
        return True
    except Exception as e:
        logger.error(f"Error querying database information: {e}")
        return False

def query_table_data(table_name, limit=5):
    """
    Query and display a sample of data from a specified table.
    
    Args:
        table_name (str): Name of the table to query
        limit (int): Number of rows to return
        
    Returns:
        DataFrame: Sample data from the table
    """
    engine = create_database_engine()
    
    if engine:
        try:
            with engine.connect() as conn:
                # Use SQLAlchemy's metadata and Table constructs to safely reference tables
                metadata = MetaData()
                
                # Reflect the table structure - this is safe from SQL injection
                table = Table(table_name, metadata, autoload_with=engine)
                
                # Build a safe query using SQLAlchemy constructs
                query = select(table).limit(limit)
                result = conn.execute(query)
                
                # Convert to DataFrame
                sample_data = pd.DataFrame(result.fetchall(), columns=result.keys())
                
                if not sample_data.empty:
                    print(f"\nSample data from {table_name}:")
                    print(sample_data)
                else:
                    print(f"No data found in {table_name}")
                
                return sample_data
        except Exception as e:
            print(f"Error querying {table_name}: {e}")
            return None
    return None

def main():
    """Main function to check database connectivity and data"""
    # Load environment variables
    load_dotenv()
    logger.info("Starting database check")
    
    # Validate environment variables
    if not validate_env_variables():
        sys.exit(1)
    
    # Create database engine
    engine = create_database_engine()
    if engine is None:
        sys.exit(1)
    
    try:
        # Query database information
        query_database_info(engine)
        
        # Table to check
        table_name = 'coin_data_structured'
        
        # Query table data
        query_table_data(table_name)
        
        logger.info("Database check completed successfully")
        
    except Exception as e:
        logger.exception(f"An error occurred during database check: {e}")
        sys.exit(1)
    finally:
        if engine:
            engine.dispose()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()








