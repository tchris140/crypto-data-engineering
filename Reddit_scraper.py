import praw
import pandas as pd
import os
import openai
import psycopg2
import json
import logging
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sqlalchemy import text
from psycopg2.extras import execute_values
from datetime import datetime, timezone, timedelta
import prawcore
import requests

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
    if MOCK_MODE:
        logger.info("Using mock database connection")
        from sqlalchemy import create_engine
        return create_engine('sqlite:///:memory:')
        
    try:
        return create_engine(
            f'postgresql://{get_env_var("DB_USER")}:{get_env_var("DB_PASSWORD")}@'
            f'{get_env_var("DB_HOST")}:{get_env_var("DB_PORT")}/{get_env_var("DB_NAME")}'
        )
    except Exception as e:
        logger.error(f"Failed to create database connection: {e}")
        raise

def get_embedding(text, model="text-embedding-ada-002"):
    """Generate embedding for a given text using OpenAI API."""
    if MOCK_MODE:
        logger.info(f"Generating mock embedding for: {text[:30]}...")
        # Return a simple mock embedding (dimensionality 1536 to match ada-002)
        return [0.1] * 1536
        
    try:
        response = openai.Embedding.create(input=[text], model=model)
        return response['data'][0]['embedding']
    except Exception as e:
        logger.error(f"Failed to generate embedding: {e}")
        raise

def fetch_recent_posts(subreddit, hours=24):
    """Fetch recent posts from subreddit within specified time window."""
    posts = []
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    try:
        for post in subreddit.new(limit=100):  # Increased limit to ensure we get enough recent posts
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
                
        return posts
    except Exception as e:
        logger.error(f"Failed to fetch posts: {e}")
        raise

def insert_posts_to_db(posts, engine):
    """Insert posts into database with error handling."""
    if not posts:
        logger.info("No new posts to insert")
        return
        
    df = pd.DataFrame(posts, columns=[
        "post_id", "title", "text", "score", 
        "num_comments", "created_utc", "embedding"
    ])
    
    try:
        # For mock mode / SQLite in-memory, create the table first if it doesn't exist
        if MOCK_MODE:
            with engine.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS reddit_embeddings (
                        post_id TEXT PRIMARY KEY,
                        title TEXT,
                        text TEXT,
                        score INTEGER,
                        num_comments INTEGER,
                        created_utc TEXT,
                        embedding TEXT
                    )
                """))
                conn.commit()
        
        conn = engine.raw_connection()
        cursor = conn.cursor()
        
        if MOCK_MODE:
            # SQLite doesn't support ON CONFLICT
            insert_query = """
                INSERT OR REPLACE INTO reddit_embeddings 
                    (post_id, title, text, score, num_comments, created_utc, embedding)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            
            records = [
                (row.post_id, row.title, row.text, row.score, 
                 row.num_comments, row.created_utc.isoformat() if hasattr(row.created_utc, 'isoformat') else str(row.created_utc), 
                 f"[{','.join(map(str, row.embedding))}]")
                for row in df.itertuples(index=False)
            ]
            
            for record in records:
                cursor.execute(insert_query, record)
        else:
            # PostgreSQL with ON CONFLICT
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
                    embedding = EXCLUDED.embedding;
            """
            
            records = [
                (row.post_id, row.title, row.text, row.score, 
                 row.num_comments, row.created_utc, 
                 f"[{','.join(map(str, row.embedding))}]")
                for row in df.itertuples(index=False)
            ]
            
            execute_values(cursor, insert_query, records)
        
        conn.commit()
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
    """Main function to orchestrate the scraping process."""
    try:
        # Check for mock mode command line argument
        if "--mock" in sys.argv:
            enable_mock_mode()
            
        # Load environment variables
        load_dotenv()
        
        # Initialize clients
        reddit = init_reddit_client()
        init_openai()
        engine = get_db_connection()
        
        # Fetch and process posts
        subreddit = reddit.subreddit("cryptocurrency")
        posts = fetch_recent_posts(subreddit)
        
        # Insert into database
        insert_posts_to_db(posts, engine)
        
        logger.info("Script completed successfully")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise

if __name__ == "__main__":
    main()

