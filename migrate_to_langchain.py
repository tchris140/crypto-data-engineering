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
from typing import List, Dict, Any
import random
import numpy as np

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

# Global mock mode flag
MOCK_MODE = False

def enable_mock_mode():
    """Enable mock mode to run without real API calls"""
    global MOCK_MODE
    MOCK_MODE = True
    logger.info("Mock mode enabled")

class MockCursor:
    """Mock database cursor for testing"""
    def __init__(self):
        self.mock_data = [
            {
                'post_id': 'post1',
                'title': 'Bitcoin price prediction',
                'text': 'I think Bitcoin will reach $100k by the end of the year.',
                'score': 42,
                'num_comments': 23,
                'created_utc': '2023-01-01',
                'embedding': str([0.1] * 1536)
            },
            {
                'post_id': 'post2',
                'title': 'Ethereum vs Solana',
                'text': 'Comparing the two smart contract platforms.',
                'score': 30,
                'num_comments': 15,
                'created_utc': '2023-01-02',
                'embedding': str([0.2] * 1536)
            },
            {
                'post_id': 'post3',
                'title': 'Regulation news',
                'text': 'New cryptocurrency regulations are coming.',
                'score': 25,
                'num_comments': 10,
                'created_utc': '2023-01-03',
                'embedding': str([0.3] * 1536)
            }
        ]
        self.fetched = 0
    
    def execute(self, query, params=None):
        logger.info(f"Mock executing query: \n{query}\n")
    
    def fetchall(self):
        logger.info("Mock fetching all results")
        return self.mock_data
    
    def fetchmany(self, size):
        logger.info(f"Mock fetching {size} results")
        if self.fetched >= len(self.mock_data):
            return []
        
        batch = self.mock_data[self.fetched:min(self.fetched + size, len(self.mock_data))]
        self.fetched += size
        return batch
    
    def close(self):
        pass

class MockConnection:
    """Mock database connection for testing"""
    def __init__(self):
        pass
    
    def cursor(self):
        return MockCursor()
    
    def commit(self):
        pass
    
    def close(self):
        pass

class MockEmbeddings:
    """Mock embeddings for testing"""
    def embed_documents(self, texts):
        return [[random.random() for _ in range(1536)] for _ in texts]
    
    def embed_query(self, text):
        return [random.random() for _ in range(1536)]

class MockVectorStore:
    """Mock vector store for LangChain"""
    def __init__(self):
        self.documents = []
    
    def add_documents(self, documents):
        self.documents.extend(documents)
        logger.info(f"Added {len(documents)} documents to mock vector store")
        return self.documents

def migrate_to_langchain(batch_size: int = 10, mock: bool = False):
    """Migrate existing Reddit embeddings to LangChain PGVector format.
    
    Args:
        batch_size: Number of documents to process at once
        mock: Whether to run in mock mode without real API calls
    """
    if mock:
        enable_mock_mode()
    
    logger.info("Beginning migration to LangChain PGVector...")
    
    # Load environment variables
    load_dotenv()
    
    # Set up database connection
    if MOCK_MODE:
        logger.info("Using mock database connection")
        conn = MockConnection()
    else:
        try:
            conn = psycopg2.connect(
                dbname=os.getenv('DB_NAME', 'crypto_data'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', 'postgres'),
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432')
            )
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            return
    
    cursor = conn.cursor()
    
    try:
        # Fetch existing Reddit embeddings
        cursor.execute("""
            SELECT post_id, title, text, embedding, score, num_comments, created_utc
            FROM reddit_embeddings
        """)
        
        # Set up LangChain components
        CONNECTION_STRING = f"postgresql+psycopg2://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'postgres')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'crypto_data')}"
        COLLECTION_NAME = "reddit_posts"
        
        # Set up embeddings
        if MOCK_MODE:
            embeddings = MockEmbeddings()
            logger.info("Using mock OpenAI embeddings")
            
            # Use mock vector store
            vector_store = MockVectorStore()
        else:
            embeddings = OpenAIEmbeddings()
            
            # Use PGVector for real migration
            try:
                logger.info("Setting up pgvector...")
                vector_store = PGVector(
                    collection_name=COLLECTION_NAME,
                    connection_string=CONNECTION_STRING,
                    embedding_function=embeddings
                )
            except Exception as e:
                logger.error(f"Error setting up pgvector: {e}")
                return
        
        # Process in batches
        if MOCK_MODE:
            # Use mock cursor for batch fetching
            all_docs = 0
            batch_num = 0
            
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                
                batch_num += 1
                logger.info(f"Processing batch {batch_num} with {len(batch)} documents")
                
                # Convert to LangChain documents
                documents = []
                for row in batch:
                    post_id, title, text, embedding_str, score, num_comments, created_utc = row
                    
                    # Create a document with metadata
                    doc = Document(
                        page_content=f"{title}\n\n{text}",
                        metadata={
                            "post_id": post_id,
                            "score": score,
                            "num_comments": num_comments,
                            "created_utc": created_utc,
                            "source": "reddit"
                        }
                    )
                    documents.append(doc)
                
                # Add to vector store
                vector_store.add_documents(documents)
                all_docs += len(documents)
            
            logger.info(f"Migration complete. {all_docs} documents migrated to LangChain PGVector.")
        else:
            # Fetch all at once for real database connection
            all_rows = cursor.fetchall()
            logger.info(f"Fetched {len(all_rows)} documents from reddit_embeddings table")
            
            # Process in batches
            for i in range(0, len(all_rows), batch_size):
                batch = all_rows[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1} with {len(batch)} documents")
                
                # Convert to LangChain documents
                documents = []
                for row in batch:
                    post_id, title, text, embedding_str, score, num_comments, created_utc = row
                    
                    # Create a document with metadata
                    doc = Document(
                        page_content=f"{title}\n\n{text}",
                        metadata={
                            "post_id": post_id,
                            "score": score,
                            "num_comments": num_comments,
                            "created_utc": created_utc,
                            "source": "reddit"
                        }
                    )
                    documents.append(doc)
                
                # Add to vector store
                vector_store.add_documents(documents)
            
            logger.info(f"Migration complete. {len(all_rows)} documents migrated to LangChain PGVector.")
    
    except Exception as e:
        logger.error(f"Error during migration: {e}")
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate Reddit embeddings to LangChain PGVector format")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of documents to process at once")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode without real API calls")
    args = parser.parse_args()
    
    migrate_to_langchain(batch_size=args.batch_size, mock=args.mock) 