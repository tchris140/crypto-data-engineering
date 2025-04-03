import unittest
from Reddit_scraper import (
    get_env_var,
    init_reddit_client,
    init_openai,
    get_db_connection,
    get_embedding,
    fetch_recent_posts,
    enable_mock_mode
)
import os
from dotenv import load_dotenv
from pathlib import Path
import sys
from sqlalchemy import text

class TestRedditScraper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment before running tests."""
        # Enable mock mode to avoid actual API calls
        enable_mock_mode()
        
        # Print current working directory for debugging
        print(f"Current working directory: {os.getcwd()}")
        
        # Try to load .env file from multiple locations
        env_locations = [
            Path('.') / '.env',  # Current directory
            Path(__file__).parent / '.env',  # Same directory as this script
            Path.home() / '.env',  # Home directory
        ]
        
        env_loaded = False
        for env_path in env_locations:
            print(f"Trying to load .env from: {env_path}")
            if env_path.exists():
                print(f"Found .env file at: {env_path}")
                load_dotenv(dotenv_path=env_path)
                env_loaded = True
                break
        
        if not env_loaded:
            print("WARNING: No .env file found in any of the expected locations.")
            print("Please ensure your .env file exists and contains the required variables.")
        
        # Check if environment variables are set directly in the environment
        required_vars = [
            "REDDIT_CLIENT_ID",
            "REDDIT_CLIENT_SECRET",
            "REDDIT_USER_AGENT",
            "OPENAI_API_KEY",
            "DB_USER",
            "DB_PASSWORD",
            "DB_HOST",
            "DB_PORT",
            "DB_NAME"
        ]
        
        # Print which variables are set and which are missing
        missing_vars = []
        for var in required_vars:
            if os.getenv(var):
                print(f"✓ {var} is set")
            else:
                print(f"✗ {var} is missing")
                missing_vars.append(var)
        
        if missing_vars:
            print("\nTo fix this issue:")
            print("1. Create a .env file in the project root directory")
            print("2. Add the following variables to your .env file:")
            for var in missing_vars:
                print(f"   {var}=your_value_here")
            print("\nOr set these variables directly in your environment.")
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
    def test_env_variables(self):
        """Test that all required environment variables are present and not empty."""
        required_vars = [
            "REDDIT_CLIENT_ID",
            "REDDIT_CLIENT_SECRET",
            "REDDIT_USER_AGENT",
            "OPENAI_API_KEY",
            "DB_USER",
            "DB_PASSWORD",
            "DB_HOST",
            "DB_PORT",
            "DB_NAME"
        ]
        
        for var in required_vars:
            value = get_env_var(var)
            self.assertIsNotNone(value, f"Environment variable {var} is not set")
            self.assertNotEqual(value, "", f"Environment variable {var} is empty")
    
    def test_reddit_client(self):
        """Test Reddit client initialization."""
        reddit = init_reddit_client()
        self.assertIsNotNone(reddit)
        
        # Test subreddit access
        subreddit = reddit.subreddit("cryptocurrency")
        self.assertIsNotNone(subreddit)
        self.assertEqual(subreddit.name, "cryptocurrency")
    
    def test_openai(self):
        """Test OpenAI initialization and embedding generation."""
        init_openai()
        
        # Test embedding generation
        test_text = "This is a test message for cryptocurrency."
        embedding = get_embedding(test_text)
        self.assertIsNotNone(embedding)
        self.assertIsInstance(embedding, list)
        self.assertTrue(len(embedding) > 0)
    
    def test_db_connection(self):
        """Test database connection."""
        engine = get_db_connection()
        self.assertIsNotNone(engine)
        
        # Test connection by executing a simple query
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            self.assertEqual(result.scalar(), 1)
    
    def test_fetch_posts(self):
        """Test post fetching functionality."""
        reddit = init_reddit_client()
        subreddit = reddit.subreddit("cryptocurrency")
        posts = fetch_recent_posts(subreddit, hours=1)  # Test with 1 hour window
        
        self.assertIsNotNone(posts)
        self.assertIsInstance(posts, list)
        self.assertTrue(len(posts) > 0, "Expected at least one post to be fetched")
        
        if posts:  # If any posts were found
            post = posts[0]
            self.assertEqual(len(post), 7, "Post should have 7 elements")
            self.assertIsInstance(post[0], str, "post_id should be a string")
            self.assertIsInstance(post[1], str, "title should be a string")
            self.assertIsInstance(post[2], str, "text should be a string")
            self.assertIsInstance(post[3], int, "score should be an integer")
            self.assertIsInstance(post[4], int, "num_comments should be an integer")
            self.assertIsInstance(post[6], list, "embedding should be a list")
    
    def test_full_workflow(self):
        """Test the entire workflow from fetching posts to database insertion."""
        reddit = init_reddit_client()
        init_openai()
        engine = get_db_connection()
        
        # Create the table for testing
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
            
        # Fetch posts
        subreddit = reddit.subreddit("cryptocurrency")
        posts = fetch_recent_posts(subreddit, hours=24)
        
        # Insert posts
        from Reddit_scraper import insert_posts_to_db
        insert_posts_to_db(posts, engine)
        
        # Verify data was inserted
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM reddit_embeddings"))
            count = result.scalar()
            self.assertTrue(count > 0, "Expected at least one row in the database")

if __name__ == '__main__':
    unittest.main() 