import pandas as pd
import os
from sqlalchemy import create_engine, text
import sys
import logging
from tabulate import tabulate
from dotenv import load_dotenv

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

def query_table_data(engine, table_name, limit=10):
    """Query and display sample data from the table"""
    try:
        # Check if table exists
        with engine.connect() as conn:
            check_query = text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                );
            """)
            exists = conn.execute(check_query).scalar()
            
            if not exists:
                logger.error(f"Table '{table_name}' does not exist in the database")
                return False
            
            # Get row count
            count_query = text(f"SELECT COUNT(*) FROM {table_name};")
            row_count = conn.execute(count_query).scalar()
            logger.info(f"Table '{table_name}' has {row_count} rows")
            
            if row_count == 0:
                logger.warning(f"Table '{table_name}' is empty")
                return True
            
            # Get column information
            columns_query = text(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position;
            """)
            columns = conn.execute(columns_query).fetchall()
            
            logger.info(f"Columns in table '{table_name}':")
            for col in columns:
                logger.info(f"  - {col[0]} ({col[1]})")
            
            # Query sample data
            logger.info(f"Sample data from table '{table_name}' (limit {limit}):")
            sample_query = text(f"SELECT * FROM {table_name} LIMIT {limit};")
            sample_data = pd.read_sql(sample_query, conn)
            
            # Display formatted table
            print("\n" + tabulate(sample_data, headers='keys', tablefmt='psql'))
            
            return True
    except Exception as e:
        logger.error(f"Error querying table data: {e}")
        return False

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
        query_table_data(engine, table_name)
        
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








