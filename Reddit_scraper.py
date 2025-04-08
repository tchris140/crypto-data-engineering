import praw
import pandas as pd
import os
import openai
import psycopg2
import json
import logging
import sys
import argparse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy import text
from psycopg2.extras import execute_values
from datetime import datetime, timezone, timedelta
import prawcore
import requests
import numpy as np

# Import data lineage
from data_lineage import get_lineage_tracker, LineageContext

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for mock mode
MOCK_MODE = False
TEST_MODE = False

def enable_mock_mode():
    """Enable mock mode for testing without API calls"""
    global MOCK_MODE
    MOCK_MODE = True
    logger.info("Mock mode enabled - no real API calls will be made")

class CustomRequestor(prawcore.Requestor):
    def request(self, *args, **kwargs):
        # Ensure headers are ASCII-compatible
        if "headers" in kwargs:
            headers = kwargs["headers"]
            ascii_headers = {}
            for key, value in headers.items():
                if isinstance(value, str):
                    ascii_headers[key] = value.encode('ascii', 'ignore').decode('ascii')
                else:
                    ascii_headers[key] = value
            kwargs["headers"] = ascii_headers
        return super().request(*args, **kwargs)

def get_env_var(var_name, required=True):
    """Safely get environment variable with error handling."""
    value = os.getenv(var_name)
    if required and not value:
        raise ValueError(f"Required environment variable {var_name} is not set")
    return value

def init_reddit_client():
    """Initialize Reddit client with error handling."""
    if MOCK_MODE:
        logger.info("Using mock Reddit client")
        return MockReddit()
        
    try:
        # Create a simple user agent string
        user_agent = "script:crypto_data_scraper:v1.0"
        
        logger.info(f"Using user agent: {user_agent}")
        
        # Create Reddit instance with custom requestor
        reddit = praw.Reddit(
            requestor_class=CustomRequestor,
            client_id=get_env_var("REDDIT_CLIENT_ID"),
            client_secret=get_env_var("REDDIT_CLIENT_SECRET"),
            user_agent=user_agent
        )
        
        return reddit
        
    except Exception as e:
        logger.error(f"Failed to initialize Reddit client: {e}")
        raise

class MockReddit:
    """Mock Reddit client for testing"""
    
    def __init__(self):
        self.subreddits = {"cryptocurrency": MockSubreddit("cryptocurrency")}
    
    def subreddit(self, name):
        return self.subreddits.get(name, MockSubreddit(name))

class MockSubreddit:
    """Mock Subreddit for testing"""
    
    def __init__(self, name):
        self.name = name
        self.display_name = name
    
    def new(self, limit=100):
        """Return mock posts"""
        # Generate a few mock posts for testing
        posts = []
        for i in range(3):
            posts.append(MockSubmission(
                id=f"mock{i}",
                title=f"Mock Post {i}",
                selftext=f"This is mock post {i} about cryptocurrency and blockchain technology.",
                score=100+i,
                num_comments=10+i,
                created_utc=(datetime.now(timezone.utc) - timedelta(hours=i)).timestamp()
            ))
        return posts

class MockSubmission:
    """Mock Submission for testing"""
    
    def __init__(self, id, title, selftext, score, num_comments, created_utc):
        self.id = id
        self.title = title
        self.selftext = selftext
        self.score = score
        self.num_comments = num_comments
        self.created_utc = created_utc

def init_openai():
    """Initialize OpenAI with error handling."""
    if MOCK_MODE:
        logger.info("Using mock OpenAI client")
        return
        
    try:
        openai.api_key = get_env_var("OPENAI_API_KEY")
    except Exception as e:
        logger.error(f"Failed to initialize OpenAI: {e}")
        raise

def get_db_connection():
    """Create database connection with error handling."""
    if MOCK_MODE or os.getenv('CI') or TEST_MODE:
        logger.info("Using mock database connection")
        return create_engine('sqlite:///:memory:')
        
    try:
        # Load environment variables
        load_dotenv()
        
        # Create PostgreSQL connection string
        conn_string = (
            f'postgresql://{get_env_var("DB_USER")}:{get_env_var("DB_PASSWORD")}@'
            f'{get_env_var("DB_HOST")}:{get_env_var("DB_PORT")}/{get_env_var("DB_NAME")}'
        )
        
        # Create and return the engine
        engine = create_engine(conn_string)
        logger.info("Successfully created PostgreSQL connection")
        return engine
        
    except Exception as e:
        logger.error(f"Failed to create database connection: {e}")
        raise

def get_embedding(text):
    """Get OpenAI embedding for text with error handling."""
    # Create a data lineage node for the embedding process
    lineage = get_lineage_tracker()
    source_id = lineage.add_node(
        node_type="dataset",
        name="Text Content",
        description="Reddit post text content to be embedded",
        metadata={"text_length": len(text) if text else 0}
    )
    
    if MOCK_MODE:
        logger.info("Using mock embeddings")
        # Generate a random vector of length 1536 (same as OpenAI embeddings)
        return np.random.rand(1536).tolist()
        
    try:
        with LineageContext(
            source_nodes=source_id,
            operation="transform",
            target_name="OpenAI Embedding",
            target_description="Text embedded as vector using OpenAI API",
            target_type="dataset",
            metadata={"model": "text-embedding-ada-002", "dimensions": 1536}
        ) as embedding_id:
            response = openai.Embedding.create(
                input=text,
                model="text-embedding-ada-002"
            )
            embedding = response['data'][0]['embedding']
            
            # Add metadata to the lineage
            lineage.get_node(embedding_id).metadata.update({
                "vector_length": len(embedding),
                "timestamp": datetime.now().isoformat()
            })
            
            return embedding
    except Exception as e:
        logger.error(f"Error getting embedding: {e}")
        raise

def fetch_recent_posts(subreddit, hours=24):
    """Fetch recent posts from subreddit within specified time window."""
    # Create a data lineage node for the Reddit API source
    lineage = get_lineage_tracker()
    source_id = lineage.add_node(
        node_type="source",
        name="Reddit API",
        description=f"API for fetching posts from r/{subreddit.display_name}",
        metadata={"subreddit": subreddit.display_name, "time_window": f"{hours} hours"}
    )
    
    posts = []
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    try:
        with LineageContext(
            source_nodes=source_id,
            operation="extract",
            target_name="Reddit Posts",
            target_description=f"Raw posts from r/{subreddit.display_name}",
            target_type="dataset",
            metadata={"timestamp": datetime.now().isoformat()}
        ) as posts_id:
            post_count = 0
            
            for post in subreddit.new(limit=100):
                created_at = datetime.fromtimestamp(float(post.created_utc), tz=timezone.utc)
                
                if created_at < cutoff_time:
                    continue
                    
                if post.selftext:
                    logger.info(f"Processing post: {post.id}")
                    embedding = get_embedding(post.selftext)
                    posts.append([
                        post.id, post.title, post.selftext, 
                        post.score, post.num_comments, created_at, embedding
                    ])
                    post_count += 1
            
            # Update metadata with post count
            node = lineage.get_node(posts_id)
            if node and isinstance(node, dict):
                node["metadata"] = node.get("metadata", {})
                node["metadata"].update({
                    "post_count": post_count,
                    "oldest_post": cutoff_time.isoformat()
                })
                # Update the node in the database
                lineage.cursor.execute("""
                    UPDATE nodes 
                    SET metadata = ? 
                    WHERE id = ?
                """, (json.dumps(node["metadata"]), posts_id))
                lineage.conn.commit()
            
            return posts
    except prawcore.exceptions.NotFound:
        logger.error("Subreddit not found. Please check the subreddit name.")
        raise
    except prawcore.exceptions.Forbidden:
        logger.error("Access to the subreddit is forbidden. Please check permissions.")
        raise
    except prawcore.exceptions.RequestException as e:
        logger.error(f"Failed to fetch posts: {e}")
        raise

def insert_posts_to_db(posts, engine):
    """Insert posts into database with vector embeddings."""
    if not posts:
        logger.info("No new posts to insert")
        return
    
    # Create data lineage nodes for the posts dataset
    lineage = get_lineage_tracker()
    posts_dataset_id = lineage.add_node(
        node_type="dataset",
        name="Reddit Posts with Embeddings",
        description="Reddit posts ready for database insertion",
        metadata={"post_count": len(posts)}
    )
    
    df = pd.DataFrame(posts, columns=[
        "post_id", "title", "text", "score", 
        "num_comments", "created_utc", "embedding"
    ])
    
    try:
        with LineageContext(
            source_nodes=posts_dataset_id,
            operation="load",
            target_name="PostgreSQL Database",
            target_description="Reddit posts stored in PostgreSQL with pgvector",
            target_type="destination",
            metadata={"timestamp": datetime.now().isoformat(), "table": "reddit_embeddings"}
        ) as db_id:
            conn = engine.raw_connection()
            cursor = conn.cursor()
            
            if MOCK_MODE or os.getenv('CI') or TEST_MODE:
                # Create a simple table for testing
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS reddit_embeddings (
                        post_id TEXT PRIMARY KEY,
                        title TEXT,
                        text TEXT,
                        score INTEGER,
                        num_comments INTEGER,
                        created_utc TIMESTAMP,
                        embedding TEXT
                    )
                """)
                conn.commit()
                
                # Simple insert query for testing
                insert_query = """
                    INSERT INTO reddit_embeddings 
                        (post_id, title, text, score, num_comments, created_utc, embedding)
                    VALUES %s
                    ON CONFLICT (post_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        text = EXCLUDED.text,
                        score = EXCLUDED.score,
                        num_comments = EXCLUDED.num_comments,
                        created_utc = EXCLUDED.created_utc,
                        embedding = EXCLUDED.embedding
                """
                
                records = [
                    (row.post_id, row.title, row.text, row.score, 
                     row.num_comments, row.created_utc, 
                     f"[{','.join(map(str, row.embedding))}]")
                    for row in df.itertuples(index=False)
                ]
                
                execute_values(cursor, insert_query, records, template="""
                    (%s, %s, %s, %s, %s, %s, %s)
                """)
            else:
                # Check if we need to add the vector column
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 
                        FROM information_schema.columns 
                        WHERE table_name = 'reddit_embeddings' 
                        AND column_name = 'embedding_vector'
                    )
                """)
                has_vector_column = cursor.fetchone()[0]
                
                if not has_vector_column:
                    logger.info("Adding embedding_vector column to reddit_embeddings table")
                    cursor.execute("ALTER TABLE reddit_embeddings ADD COLUMN embedding_vector vector(1536)")
                
                # Create table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS reddit_embeddings (
                        post_id TEXT PRIMARY KEY,
                        title TEXT,
                        text TEXT,
                        score INTEGER,
                        num_comments INTEGER,
                        created_utc TIMESTAMP,
                        embedding TEXT,
                        embedding_vector vector(1536)
                    )
                """)
                conn.commit()
                
                # PostgreSQL with vector support
                insert_query = """
                    INSERT INTO reddit_embeddings 
                        (post_id, title, text, score, num_comments, created_utc, embedding, embedding_vector)
                    VALUES %s
                    ON CONFLICT (post_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        text = EXCLUDED.text,
                        score = EXCLUDED.score,
                        num_comments = EXCLUDED.num_comments,
                        created_utc = EXCLUDED.created_utc,
                        embedding = EXCLUDED.embedding,
                        embedding_vector = EXCLUDED.embedding_vector
                """
                
                records = [
                    (row.post_id, row.title, row.text, row.score, 
                     row.num_comments, row.created_utc, 
                     f"[{','.join(map(str, row.embedding))}]",
                     f"[{','.join(map(str, row.embedding))}]")
                    for row in df.itertuples(index=False)
                ]
                
                execute_values(cursor, insert_query, records, template="""
                    (%s, %s, %s, %s, %s, %s, %s, %s::vector)
                """)
            
            conn.commit()
            
            # Add metadata to the lineage
            lineage.get_node(db_id).metadata.update({
                "records_inserted": len(posts)
            })
            
            logger.info(f"Successfully inserted {len(posts)} posts into database")
        
    except Exception as e:
        logger.error(f"Failed to insert posts into database: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    """Main function to orchestrate the Reddit scraping process."""
    parser = argparse.ArgumentParser(description='Reddit Crypto Scraper')
    parser.add_argument('--mock', action='store_true', help='Run in mock mode without real API calls')
    parser.add_argument('--test', action='store_true', help='Run in test mode with simplified database operations')
    args = parser.parse_args()
    
    if args.mock:
        enable_mock_mode()
    
    if args.test:
        global TEST_MODE
        TEST_MODE = True
        logger.info("Test mode enabled - using simplified database operations")
    
    try:
        # Initialize clients
        reddit = init_reddit_client()
        init_openai()
        engine = get_db_connection()
        
        # Create a data lineage pipeline node
        lineage = get_lineage_tracker()
        pipeline_id = lineage.add_node(
            node_type="transformation",
            name="Reddit Pipeline",
            description="Pipeline for scraping Reddit posts",
            metadata={"timestamp": datetime.now().isoformat()}
        )
        
        # Fetch posts from r/cryptocurrency
        logger.info("Fetching posts from r/cryptocurrency")
        posts = fetch_recent_posts(reddit.subreddit("cryptocurrency"))
        
        # Insert posts to database
        insert_posts_to_db(posts, engine)
        
        # Generate lineage visualization
        lineage.visualize()
        lineage.export_json()
        
        logger.info("Script completed successfully")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

