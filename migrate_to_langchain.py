"""
Migration script to convert existing Reddit embeddings to LangChain PGVector format.

This script will:
1. Read existing embeddings from the reddit_embeddings table
2. Convert them to LangChain Document format
3. Store them in a format compatible with LangChain's PGVector
"""

import argparse
import ast
import logging
import os
from typing import List

import psycopg2
from dotenv import load_dotenv
from langchain_community.embeddings import OpenAIEmbeddings
from langchain.schema import Document
from langchain_community.vectorstores import PGVector

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for mock mode
MOCK_MODE = False

def enable_mock_mode():
    """Enable mock mode for testing without API calls"""
    global MOCK_MODE
    MOCK_MODE = True
    logger.info("Mock mode enabled - no real API calls will be made")

# Load environment variables from .env file
load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_port = int(os.getenv('DB_PORT', '5432'))  # Default to 5432 if not set
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

# OpenAI API key
openai_api_key = os.getenv('OPENAI_API_KEY')

# Configure connection string for pgvector
CONNECTION_STRING = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def get_db_connection():
    """Create database connection with error handling"""
    if MOCK_MODE:
        logger.info("Using mock database connection for migration")
        # Create a mock connection object that mimics psycopg2 connection
        class MockCursor:
            def execute(self, query, params=None):
                logger.info(f"Mock executing query: {query}")
                return self
                
            def fetchall(self):
                logger.info("Mock fetching results")
                # Mock Reddit embeddings
                return [
                    ["mock1", "Ethereum Discussion", "Ethereum is a great blockchain platform with smart contracts.", "[0.1]" * 1536, 100, 50, "2023-01-01"],
                    ["mock2", "Bitcoin vs Ethereum", "Comparing the two biggest cryptocurrencies.", "[0.2]" * 1536, 200, 75, "2023-01-02"],
                    ["mock3", "Solana's Recent Growth", "Solana has seen massive adoption recently.", "[0.3]" * 1536, 150, 30, "2023-01-03"]
                ]
                
            def fetchone(self):
                logger.info("Mock fetching one result")
                return [True]  # Pretend the operation succeeded
                
            def close(self):
                pass
                
        class MockConnection:
            def cursor(self):
                return MockCursor()
                
            def close(self):
                pass
                
        return MockConnection()
    
    try:
        return psycopg2.connect(
            dbname=db_name, 
            user=db_user, 
            password=db_password, 
            host=db_host, 
            port=db_port
        )
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def fetch_existing_embeddings() -> List[Document]:
    """
    Fetch existing Reddit embeddings from the database and 
    convert them to LangChain Document format.
    
    Returns:
        List of LangChain Document objects containing Reddit posts and embeddings
    """
    logger.info("Fetching existing Reddit embeddings from database...")
    
    db_connection = get_db_connection()
    cursor = db_connection.cursor()
    
    try:
        # Query to fetch all embeddings from the Reddit posts
        cursor.execute("SELECT post_id, title, text, embedding, score, num_comments, created_utc FROM reddit_embeddings")
        rows = cursor.fetchall()
        
        if not rows:
            logger.warning("No existing embeddings found in the database.")
            return []
        
        # Convert to LangChain Document format
        documents = []
        
        for row in rows:
            post_id, title, text, embedding_str, score, num_comments, created_utc = row
            
            # Create a Document for each post
            doc = Document(
                page_content=text,
                metadata={
                    "post_id": post_id,
                    "title": title,
                    "score": score,
                    "num_comments": num_comments,
                    "created_utc": created_utc
                }
            )
            
            # If we need the raw embedding, we can add it (for direct import without re-embedding)
            # For this example, we'll re-embed to ensure compatibility with LangChain
            # doc.metadata["embedding"] = ast.literal_eval(embedding_str)
            
            documents.append(doc)
        
        logger.info(f"Successfully fetched {len(documents)} documents from the database.")
        return documents
        
    except Exception as e:
        logger.error(f"Error fetching embeddings: {e}")
        return []
    finally:
        cursor.close()
        db_connection.close()

def create_pgvector_table():
    """
    Create or verify the pgvector table needed for LangChain.
    This ensures the extension is installed and the collection table exists.
    """
    logger.info("Setting up pgvector for LangChain...")
    
    if MOCK_MODE:
        logger.info("Mock mode: Simulating pgvector setup")
        return True
    
    db_connection = get_db_connection()
    cursor = db_connection.cursor()
    
    try:
        # Check if pgvector extension is installed
        cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')")
        has_vector_extension = cursor.fetchone()[0]
        
        if not has_vector_extension:
            logger.error("pgvector extension is not installed in the database.")
            logger.info("Please run: CREATE EXTENSION IF NOT EXISTS vector;")
            return False
        
        # The langchain_pg_embedding table will be created automatically by PGVector
        # but we can verify it doesn't already exist to avoid conflicts
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'langchain_pg_embedding')")
        has_table = cursor.fetchone()[0]
        
        if has_table:
            logger.info("langchain_pg_embedding table already exists.")
            
            # Optional: Check if it has content already
            cursor.execute("SELECT COUNT(*) FROM langchain_pg_embedding")
            count = cursor.fetchone()[0]
            if count > 0:
                logger.warning(f"Table already contains {count} records.")
                
        logger.info("pgvector setup complete.")
        return True
        
    except Exception as e:
        logger.error(f"Error setting up pgvector: {e}")
        return False
    finally:
        cursor.close()
        db_connection.close()

class MockVectorStore:
    """Mock implementation of the vector store for testing"""
    
    def __init__(self):
        self.documents = []
        logger.info("Initialized mock vector store")
    
    def add_documents(self, documents):
        """Simulate adding documents to the vector store"""
        self.documents.extend(documents)
        logger.info(f"Added {len(documents)} documents to mock vector store")
        return len(documents)

def migrate_to_langchain(batch_size=100, mock=False):
    """
    Migrate existing Reddit embeddings to LangChain PGVector format.
    
    Args:
        batch_size: Number of documents to process in each batch
        mock: Whether to run in mock mode
    """
    if mock:
        enable_mock_mode()
        
    logger.info("Starting migration to LangChain PGVector...")
    
    # Initialize OpenAI embeddings
    if not MOCK_MODE:
        embeddings = OpenAIEmbeddings(api_key=openai_api_key)
    else:
        # Create a simple mock embeddings object
        class MockEmbeddings:
            def embed_documents(self, texts):
                return [[0.1] * 1536 for _ in texts]
            
            def embed_query(self, text):
                return [0.1] * 1536
                
        embeddings = MockEmbeddings()
    
    # Make sure pgvector is set up
    if not create_pgvector_table():
        logger.error("Failed to set up pgvector. Migration aborted.")
        return
    
    # Fetch documents from the existing database
    documents = fetch_existing_embeddings()
    
    if not documents:
        logger.warning("No documents to migrate. Exiting.")
        return
    
    # Set up LangChain's PGVector or mock version
    try:
        if MOCK_MODE:
            vectorstore = MockVectorStore()
        else:
            vectorstore = PGVector(
                collection_name="reddit_vectors",
                connection_string=CONNECTION_STRING,
                embedding_function=embeddings,
                use_jsonb=True
            )
        
        # Process in batches to avoid memory issues or rate limiting
        total_documents = len(documents)
        for i in range(0, total_documents, batch_size):
            batch = documents[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(total_documents-1)//batch_size + 1} ({len(batch)} documents)")
            
            # Add documents to the vector store
            vectorstore.add_documents(batch)
            
            logger.info(f"Batch {i//batch_size + 1} completed.")
        
        logger.info(f"Migration complete! {total_documents} documents migrated to LangChain PGVector.")
        
    except Exception as e:
        logger.error(f"Error during migration: {e}")

def main():
    """Main function to run the migration script"""
    parser = argparse.ArgumentParser(description='Migrate Reddit embeddings to LangChain PGVector')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing documents')
    parser.add_argument('--mock', action='store_true', help='Run in mock mode without real API calls')
    args = parser.parse_args()
    
    migrate_to_langchain(batch_size=args.batch_size, mock=args.mock)

if __name__ == "__main__":
    main() 