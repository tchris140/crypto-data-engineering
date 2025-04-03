import psycopg2
import logging
import os
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_env_var(var_name, required=True):
    """Safely get environment variable with error handling."""
    value = os.getenv(var_name)
    if required and not value:
        raise ValueError(f"Required environment variable {var_name} is not set")
    return value

def get_db_connection():
    """Create database connection with error handling."""
    try:
        return create_engine(
            f'postgresql://{get_env_var("DB_USER")}:{get_env_var("DB_PASSWORD")}@'
            f'{get_env_var("DB_HOST")}:{get_env_var("DB_PORT")}/{get_env_var("DB_NAME")}'
        )
    except Exception as e:
        logger.error(f"Failed to create database connection: {e}")
        raise

def test_pgvector_extension():
    """Test if the pgvector extension is properly installed."""
    try:
        conn = get_db_connection().raw_connection()
        cursor = conn.cursor()
        
        # Check if vector extension exists
        cursor.execute("SELECT extname FROM pg_extension WHERE extname = 'vector'")
        result = cursor.fetchone()
        
        if result and result[0] == 'vector':
            logger.info("✅ pgvector extension is installed")
            return True
        else:
            logger.warning("❌ pgvector extension is NOT installed")
            logger.info("Installing pgvector extension...")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
            conn.commit()
            logger.info("✅ pgvector extension has been installed")
            return True
    except Exception as e:
        logger.error(f"Failed to verify pgvector extension: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def test_table_schema():
    """Test if the reddit_embeddings table has the embedding_vector column."""
    try:
        engine = get_db_connection()
        conn = engine.raw_connection()
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'reddit_embeddings')")
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.warning("❌ reddit_embeddings table does not exist")
            logger.info("Creating reddit_embeddings table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reddit_embeddings (
                    post_id TEXT PRIMARY KEY,
                    title TEXT,
                    text TEXT,
                    score INTEGER,
                    num_comments INTEGER,
                    created_utc TEXT,
                    embedding TEXT,
                    embedding_vector vector(1536)
                )
            """)
            conn.commit()
            logger.info("✅ reddit_embeddings table has been created")
        else:
            logger.info("✅ reddit_embeddings table exists")
        
        # Check if embedding_vector column exists
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'reddit_embeddings' AND column_name = 'embedding_vector')")
        column_exists = cursor.fetchone()[0]
        
        if column_exists:
            logger.info("✅ embedding_vector column exists")
        else:
            logger.warning("❌ embedding_vector column does NOT exist")
            logger.info("Adding embedding_vector column...")
            cursor.execute("ALTER TABLE reddit_embeddings ADD COLUMN embedding_vector vector(1536)")
            conn.commit()
            logger.info("✅ embedding_vector column has been added")
        
        return True
    except Exception as e:
        logger.error(f"Failed to verify table schema: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def test_vector_search():
    """Test vector search functionality with a sample query."""
    try:
        engine = get_db_connection()
        conn = engine.raw_connection()
        cursor = conn.cursor()
        
        # Check if there are any records with embedding_vector
        cursor.execute("SELECT COUNT(*) FROM reddit_embeddings WHERE embedding_vector IS NOT NULL")
        count = cursor.fetchone()[0]
        
        if count == 0:
            logger.warning("❌ No records with embedding_vector found")
            logger.info("Inserting a test record...")
            
            # Create a mock embedding vector
            mock_embedding = np.random.rand(1536).tolist()
            
            # Insert a test record
            cursor.execute("""
                INSERT INTO reddit_embeddings 
                (post_id, title, text, score, num_comments, created_utc, embedding_vector)
                VALUES (%s, %s, %s, %s, %s, %s, %s::vector)
                ON CONFLICT (post_id) DO UPDATE SET embedding_vector = EXCLUDED.embedding_vector
            """, (
                "test_post", "Test Title", "This is a test post about cryptocurrency.", 
                100, 10, "2023-01-01T00:00:00Z", mock_embedding
            ))
            conn.commit()
            logger.info("✅ Test record inserted")
            
            # Update the count
            count = 1
        
        logger.info(f"Found {count} records with embedding_vector")
        
        # Test vector search
        logger.info("Testing vector search...")
        
        # Create a query vector
        query_vector = np.random.rand(1536).tolist()
        
        # Perform vector search using the <-> operator (cosine distance)
        cursor.execute("""
            SELECT post_id, title, embedding_vector <-> %s::vector as distance
            FROM reddit_embeddings
            WHERE embedding_vector IS NOT NULL
            ORDER BY distance
            LIMIT 3
        """, (query_vector,))
        
        results = cursor.fetchall()
        
        if results:
            logger.info("✅ Vector search successful!")
            logger.info(f"Found {len(results)} similar posts:")
            for post_id, title, distance in results:
                similarity = 1.0 - distance  # Convert distance to similarity
                logger.info(f"- {post_id}: '{title}' (similarity: {similarity:.4f})")
            return True
        else:
            logger.warning("❌ Vector search returned no results")
            return False
    except Exception as e:
        logger.error(f"Failed to test vector search: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    """Main function to run the tests."""
    logger.info("Testing pgvector integration...")
    
    # Load environment variables
    load_dotenv()
    
    # Run tests
    extension_ok = test_pgvector_extension()
    if not extension_ok:
        logger.error("Failed to verify pgvector extension. Please install it manually.")
        return False
    
    schema_ok = test_table_schema()
    if not schema_ok:
        logger.error("Failed to verify table schema. Please check your database setup.")
        return False
    
    search_ok = test_vector_search()
    if not search_ok:
        logger.error("Failed to perform vector search. Please check your data and code.")
        return False
    
    logger.info("✅ All tests passed! pgvector integration is working correctly.")
    return True

if __name__ == "__main__":
    main() 