import psycopg2
import openai
import numpy as np
import os
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv
import ast  # For converting string to list

# Load environment variables from .env file
load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_port = int(os.getenv('DB_PORT'))  # Ensure DB_PORT is treated as an integer
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

# OpenAI API key
openai.api_key = os.getenv('OPENAI_API_KEY')

# Function to generate OpenAI embeddings
def get_embedding(text, model="text-embedding-ada-002"):
    """Generate embedding for a given text using OpenAI API."""
    response = openai.Embedding.create(input=[text], model=model)
    return response['data'][0]['embedding']

# Function to retrieve relevant Reddit data (unstructured)
def retrieve_reddit_data(query, db_connection):
    # Generate the embedding for the query using OpenAI
    query_embedding = get_embedding(query)
    
    # Connect to the database
    cursor = db_connection.cursor()
    
    # Query to fetch embeddings from the Reddit posts
    cursor.execute("SELECT post_id, title, text, embedding FROM reddit_embeddings")
    rows = cursor.fetchall()
    
    # Extract embeddings and calculate similarity
    embeddings = np.array([np.array(ast.literal_eval(row[3])) for row in rows])  # Convert string to list
    similarities = cosine_similarity([query_embedding], embeddings)[0]
    
    # Find the most similar post(s)
    most_similar_idx = similarities.argmax()  # Get index of highest similarity
    most_similar_post = rows[most_similar_idx]
    
    # Format the response from Reddit post
    return f"Here's a popular discussion about your topic:\n\n**{most_similar_post[1]}**\n{most_similar_post[2]}\n\n*Reddit users seem to be discussing this topic in-depth.*"

# Function to retrieve relevant structured data (crypto prices, market cap, etc.)
def retrieve_structured_data(query, db_connection):
    # Connect to the database
    cursor = db_connection.cursor()

    # Define your query to fetch the relevant structured data
    cursor.execute("""
        SELECT "Name", "Symbol", "Price (USD)", "Market Cap (USD)", "24h Volume (USD)", "Circulating Supply" 
        FROM coin_data_structured
        WHERE "Name" ILIKE %s OR "Symbol" ILIKE %s
    """, (query, query))
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
    # First, try retrieving relevant structured data
    structured_response = retrieve_structured_data(query, db_connection)
    
    # Second, try retrieving relevant Reddit data
    reddit_response = retrieve_reddit_data(query, db_connection)
    
    # Combine the responses into a more conversational chat-like format
    response = f"Sure! Here's what I found:\n\n{structured_response}\n\n---\n\n{reddit_response}"
    
    return response


# Example usage:
# Connect to the database
db_connection = psycopg2.connect(
    dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
)

# Get user query
query = "Ethereum"

# Get the chat response
response = chat(query, db_connection)

# Display the response
print(response)


# # Ask the user for a query
# query = input("What would you like to know? ")

# # Retrieve relevant data from the database
# relevant_data, similarity, source = retrieve_relevant_data(query, db_connection)

# # Provide the response to the user
# print(f"Most Relevant Information: {relevant_data}")
# print(f"Similarity Score: {similarity:.4f}")
# print(f"Source: {source}")