import praw
import pandas as pd
import os
import openai
import psycopg2
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sqlalchemy import text
from psycopg2.extras import execute_values
from datetime import datetime, timezone


# Load environment variables
load_dotenv()

# Reddit API credentials
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT")
)

# OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

# PostgreSQL connection details
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
table_name = "reddit_embeddings"

# Create PostgreSQL connection
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Function to generate OpenAI embeddings
def get_embedding(text, model="text-embedding-ada-002"):
    """Generate embedding for a given text using OpenAI API."""
    response = openai.Embedding.create(input=[text], model=model)
    return response['data'][0]['embedding']

# Fetch posts from the "cryptocurrency" subreddit
subreddit = reddit.subreddit("cryptocurrency")
posts = []

for post in subreddit.hot(limit=50):  # Change limit as needed
    if post.selftext:  # Ensure the post has text
        print(f"Post ID: {post.id}, created_utc: {post.created_utc}, type: {type(post.created_utc)}")
        
        created_at = datetime.fromtimestamp(float(post.created_utc), tz=timezone.utc)
        embedding = get_embedding(post.selftext)  # ✅ Call get_embedding() here
        
        posts.append([post.id, post.title, post.selftext, post.score, post.num_comments, created_at, embedding])




# Convert to DataFrame
df = pd.DataFrame(posts, columns=["post_id", "title", "text", "score", "num_comments", "created_utc", "embedding"])

# Insert data into PostgreSQL using psycopg2 for efficiency
try:
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cursor = conn.cursor()

    # Define the INSERT query
    insert_query = """
        INSERT INTO reddit_embeddings (post_id, title, text, score, num_comments, created_utc, embedding)
        VALUES %s
        ON CONFLICT (post_id) DO UPDATE SET
        title = EXCLUDED.title,
        text = EXCLUDED.text,
        score = EXCLUDED.score,
        num_comments = EXCLUDED.num_comments,
        created_utc = EXCLUDED.created_utc,
        embedding = EXCLUDED.embedding;
    """


    # Convert embeddings to JSON format for PostgreSQL
    records = [(row.post_id, row.title, row.text, row.score, row.num_comments, row.created_utc, f"[{','.join(map(str, row.embedding))}]")
               for row in df.itertuples(index=False)]

    # Bulk insert
    execute_values(cursor, insert_query, records)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Data successfully inserted into PostgreSQL!")

except Exception as e:
    print(f"❌ An error occurred: {e}")

