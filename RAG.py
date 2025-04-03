import psycopg2
import openai
import numpy as np
import os
import argparse
import logging
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv
import ast  # For converting string to list

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
openai.api_key = os.getenv('OPENAI_API_KEY')

# Function to generate OpenAI embeddings
def get_embedding(text, model="text-embedding-ada-002"):
    """Generate embedding for a given text using OpenAI API."""
    if MOCK_MODE:
        logger.info(f"Generating mock embedding for: {text[:30]}...")
        # Return a simple mock embedding (dimensionality 1536 to match ada-002)
        return [0.1] * 1536
        
    response = openai.Embedding.create(input=[text], model=model)
    return response['data'][0]['embedding']

# Function to get database connection
def get_db_connection():
    """Create database connection with error handling and mock support."""
    if MOCK_MODE:
        logger.info("Using mock database connection")
        # Create a mock connection object that mimics psycopg2 connection
        class MockCursor:
            def execute(self, query, params=None):
                logger.info(f"Mock executing query: {query}")
                return self
                
            def fetchall(self):
                logger.info("Mock fetching results")
                if "reddit_embeddings" in self.last_query:
                    # Mock Reddit embeddings
                    return [
                        ["mock1", "Ethereum Discussion", "Ethereum is a great blockchain platform with smart contracts.", "[" + ",".join(["0.1"] * 1536) + "]"],
                        ["mock2", "Bitcoin vs Ethereum", "Comparing the two biggest cryptocurrencies.", "[" + ",".join(["0.2"] * 1536) + "]"]
                    ]
                else:
                    # Mock structured coin data
                    return [["Ethereum", "ETH", 3500.45, 420000000000, 15000000000, 120000000]]
                    
            def __init__(self):
                self.last_query = ""
                
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

# Function to retrieve relevant Reddit data (unstructured)
def retrieve_reddit_data(query, db_connection):
    # Generate the embedding for the query using OpenAI
    query_embedding = get_embedding(query)
    
    # Connect to the database
    cursor = db_connection.cursor()
    
    # Query to fetch embeddings from the Reddit posts
    cursor.execute("SELECT post_id, title, text, embedding FROM reddit_embeddings")
    rows = cursor.fetchall()
    
    if not rows:
        return "No Reddit discussions found on this topic."
    
    # Extract embeddings and calculate similarity
    try:
        embeddings = np.array([np.array(ast.literal_eval(row[3])) for row in rows])  # Convert string to list
        similarities = cosine_similarity([query_embedding], embeddings)[0]
        
        # Find the most similar post(s)
        most_similar_idx = similarities.argmax()  # Get index of highest similarity
        most_similar_post = rows[most_similar_idx]
        
        # Format the response from Reddit post
        return f"Here's a popular discussion about your topic:\n\n**{most_similar_post[1]}**\n{most_similar_post[2]}\n\n*Reddit users seem to be discussing this topic in-depth.*"
    except Exception as e:
        logger.error(f"Error processing Reddit data: {e}")
        return "I had trouble processing the Reddit discussions. Let's focus on the market data for now."

# Function to retrieve relevant structured data (crypto prices, market cap, etc.)
def retrieve_structured_data(query, db_connection):
    # Connect to the database
    cursor = db_connection.cursor()

    # Define your query to fetch the relevant structured data
    cursor.execute("""
        SELECT "Name", "Symbol", "Price (USD)", "Market Cap (USD)", "24h Volume (USD)", "Circulating Supply" 
        FROM coin_data_structured
        WHERE "Name" ILIKE %s OR "Symbol" ILIKE %s
    """, (f"%{query}%", f"%{query}%"))
    rows = cursor.fetchall()
    
    # Format the structured data response
    if rows:
        data = rows[0]  # Assuming we're looking for the first matching coin
        return f"Here are the details for {data[0]} ({data[1]}):\n" \
               f"Price: ${data[2]:,.2f}\n" \
               f"Market Cap: ${data[3]:,.2f}\n" \
               f"24h Volume: ${data[4]:,.2f}\n" \
               f"Circulating Supply: {data[5]:,.0f} {data[1]}"
    else:
        return "Sorry, I couldn't find any data for that coin."

# Main function to handle the query and return a combined response
def chat(query, db_connection):
    logger.info(f"Processing query: {query}")
    
    # First, try retrieving relevant structured data
    structured_response = retrieve_structured_data(query, db_connection)
    
    # Second, try retrieving relevant Reddit data
    reddit_response = retrieve_reddit_data(query, db_connection)
    
    # Combine the responses into a more conversational chat-like format
    response = f"Sure! Here's what I found:\n\n{structured_response}\n\n---\n\n{reddit_response}"
    
    return response

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='RAG System for Crypto Data')
    parser.add_argument('--mock', action='store_true', help='Run in mock mode without real API calls')
    parser.add_argument('--query', type=str, default="Ethereum", help='Query to process')
    args = parser.parse_args()
    
    if args.mock:
        enable_mock_mode()
    
    # Connect to the database
    db_connection = get_db_connection()
    
    # Get user query
    query = args.query
    
    # Get the chat response
    response = chat(query, db_connection)
    
    # Display the response
    print("\n" + "="*50)
    print(response)
    print("="*50 + "\n")
    
if __name__ == "__main__":
    main()


# # Ask the user for a query
# query = input("What would you like to know? ")

# # Retrieve relevant data from the database
# relevant_data, similarity, source = retrieve_relevant_data(query, db_connection)

# # Provide the response to the user
# print(f"Most Relevant Information: {relevant_data}")
# print(f"Similarity Score: {similarity:.4f}")
# print(f"Source: {source}")