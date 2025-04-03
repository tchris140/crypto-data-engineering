import psycopg2
import openai
import numpy as np
import os
import argparse
import logging
import json
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv
import ast  # For converting string to list
import pandas as pd
from datetime import datetime

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
                self.last_query = query
                return self
                
            def fetchall(self):
                logger.info("Mock fetching results")
                if "reddit_embeddings" in self.last_query:
                    # Mock Reddit embeddings
                    return [
                        ["mock1", "Ethereum Discussion", "Ethereum is a great blockchain platform with smart contracts.", "[" + ",".join(["0.1"] * 1536) + "]"],
                        ["mock2", "Bitcoin vs Ethereum", "Comparing the two biggest cryptocurrencies. Ethereum has more use cases with its smart contract platform, but Bitcoin remains the largest by market cap.", "[" + ",".join(["0.2"] * 1536) + "]"],
                        ["mock3", "Solana's Recent Growth", "Solana has seen massive adoption recently due to its high throughput and low fees.", "[" + ",".join(["0.3"] * 1536) + "]"]
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
def retrieve_reddit_data(query, db_connection, top_k=3):
    """Retrieve top k most relevant Reddit posts based on semantic similarity using pgvector."""
    # Create data lineage nodes
    lineage = get_lineage_tracker()
    query_node_id = lineage.add_node(
        node_type="dataset",
        name="User Query",
        description="User query for RAG system",
        metadata={"query": query, "timestamp": datetime.now().isoformat()}
    )
    
    # Generate the embedding for the query using OpenAI
    query_embedding = get_embedding(query)
    
    # Add embedding node in lineage
    embedding_node_id = lineage.add_node(
        node_type="dataset",
        name="Query Embedding",
        description="Vector embedding of the user query",
        metadata={"vector_length": len(query_embedding), "timestamp": datetime.now().isoformat()}
    )
    
    # Add edge from query to embedding
    lineage.add_edge(
        source_id=query_node_id,
        target_id=embedding_node_id,
        operation="embed",
        metadata={"model": "text-embedding-ada-002"}
    )
    
    # Connect to the database
    cursor = db_connection.cursor()
    
    try:
        # First check if our table is using the vector column
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'reddit_embeddings' AND column_name = 'embedding_vector')")
        has_vector_column = cursor.fetchone()[0]
        
        # Create database source node in lineage
        db_source_id = lineage.add_node(
            node_type="source",
            name="PostgreSQL Database",
            description="Database containing Reddit posts with embeddings",
            metadata={"table": "reddit_embeddings", "has_vector_column": has_vector_column}
        )
        
        with LineageContext(
            source_nodes=[embedding_node_id, db_source_id],
            operation="retrieve",
            target_name="Similar Reddit Posts",
            target_description="Reddit posts most similar to the query",
            target_type="dataset",
            metadata={"top_k": top_k, "timestamp": datetime.now().isoformat()}
        ) as results_id:
            
            if has_vector_column:
                logger.info("Using pgvector for similarity search")
                # Use pgvector's cosine distance operator (<->) for similarity search
                cursor.execute("""
                    SELECT post_id, title, text, 1 - (embedding_vector <-> %s::vector) as similarity 
                    FROM reddit_embeddings
                    WHERE embedding_vector IS NOT NULL
                    ORDER BY embedding_vector <-> %s::vector
                    LIMIT %s
                """, (query_embedding, query_embedding, top_k))
                
                rows = cursor.fetchall()
                
                if rows:
                    top_posts = []
                    for row in rows:
                        top_posts.append({
                            "post_id": row[0],
                            "title": row[1],
                            "content": row[2],
                            "similarity": row[3]
                        })
                    
                    # Update lineage metadata
                    lineage.get_node(results_id).metadata.update({
                        "posts_found": len(top_posts),
                        "max_similarity": max([post["similarity"] for post in top_posts]) if top_posts else 0,
                        "search_method": "pgvector"
                    })
                    
                    logger.info(f"Found {len(top_posts)} similar posts using pgvector")
                    return top_posts
            
            # Fallback to the original in-memory approach if pgvector is not available
            logger.info("Falling back to in-memory similarity calculation")
            cursor.execute("SELECT post_id, title, text, embedding FROM reddit_embeddings")
            rows = cursor.fetchall()
            
            if not rows:
                # Update lineage metadata to show no results
                lineage.get_node(results_id).metadata.update({
                    "posts_found": 0,
                    "search_method": "fallback",
                    "error": "No Reddit posts found in database"
                })
                return []
            
            embeddings = np.array([np.array(ast.literal_eval(row[3])) for row in rows])  # Convert string to list
            similarities = cosine_similarity([query_embedding], embeddings)[0]
            
            # Find the most similar posts
            top_indices = similarities.argsort()[-top_k:][::-1]  # Get indices of top k similarity scores
            top_posts = []
            
            for idx in top_indices:
                post = rows[idx]
                top_posts.append({
                    "post_id": post[0],
                    "title": post[1],
                    "content": post[2],
                    "similarity": similarities[idx]
                })
            
            # Update lineage metadata
            lineage.get_node(results_id).metadata.update({
                "posts_found": len(top_posts),
                "max_similarity": max([post["similarity"] for post in top_posts]) if top_posts else 0,
                "search_method": "in-memory"
            })
            
            return top_posts
    except Exception as e:
        logger.error(f"Error retrieving Reddit data: {e}")
        return []

# Function to retrieve relevant structured data (crypto prices, market cap, etc.)
def retrieve_structured_data(query, db_connection):
    """Retrieve structured market data for cryptocurrencies matching the query."""
    # Create data lineage nodes
    lineage = get_lineage_tracker()
    query_node_id = lineage.add_node(
        node_type="dataset",
        name="Market Data Query",
        description="Query for structured cryptocurrency market data",
        metadata={"query": query, "timestamp": datetime.now().isoformat()}
    )
    
    # Connect to the database
    cursor = db_connection.cursor()
    
    # Create database source node in lineage
    db_source_id = lineage.add_node(
        node_type="source",
        name="PostgreSQL Database",
        description="Database containing cryptocurrency market data",
        metadata={"table": "coin_data_structured"}
    )
    
    with LineageContext(
        source_nodes=[query_node_id, db_source_id],
        operation="retrieve",
        target_name="Market Data Results",
        target_description="Structured cryptocurrency market data matching the query",
        target_type="dataset",
        metadata={"timestamp": datetime.now().isoformat()}
    ) as results_id:
        # Define your query to fetch the relevant structured data
        cursor.execute("""
            SELECT "Name", "Symbol", "Price (USD)", "Market Cap (USD)", "24h Volume (USD)", "Circulating Supply" 
            FROM coin_data_structured
            WHERE "Name" ILIKE %s OR "Symbol" ILIKE %s
        """, (f"%{query}%", f"%{query}%"))
        rows = cursor.fetchall()
        
        # Format the structured data 
        if rows:
            structured_data = []
            for row in rows:
                structured_data.append({
                    "name": row[0],
                    "symbol": row[1],
                    "price": row[2],
                    "market_cap": row[3],
                    "volume_24h": row[4],
                    "circulating_supply": row[5]
                })
            
            # Update lineage metadata
            lineage.get_node(results_id).metadata.update({
                "coins_found": len(structured_data),
                "coins": [item["symbol"] for item in structured_data]
            })
            
            return structured_data
        else:
            # Update lineage metadata to show no results
            lineage.get_node(results_id).metadata.update({
                "coins_found": 0,
                "error": "No matching cryptocurrency data found"
            })
            
            return []

def generate_conversational_response(query, structured_data, reddit_posts):
    """Generate a conversational response using LLM based on retrieved data."""
    # Create data lineage nodes
    lineage = get_lineage_tracker()
    
    # Create nodes for the input data
    structured_node_id = lineage.add_node(
        node_type="dataset",
        name="Structured Market Data",
        description="Structured cryptocurrency market data for LLM context",
        metadata={"coin_count": len(structured_data)}
    )
    
    reddit_node_id = lineage.add_node(
        node_type="dataset",
        name="Reddit Posts",
        description="Reddit posts for LLM context",
        metadata={"post_count": len(reddit_posts)}
    )
    
    with LineageContext(
        source_nodes=[structured_node_id, reddit_node_id],
        operation="generate",
        target_name="LLM Response",
        target_description="Conversational response generated by LLM",
        target_type="dataset",
        metadata={"query": query, "timestamp": datetime.now().isoformat()}
    ) as response_id:
        
        if MOCK_MODE:
            logger.info("Generating mock conversational response")
            mock_response = f"""
    I'd be happy to tell you about {query}!
    
    Based on our latest market data, Ethereum (ETH) is currently trading at $3,500.45 with a market cap of over $420 billion. The 24-hour trading volume is approximately $15 billion, and there are about 120 million ETH in circulation.
    
    Looking at recent discussions, the crypto community has been talking about Ethereum's smart contract capabilities, which continue to make it a foundational blockchain for decentralized applications. There's also been some comparison between Ethereum and Bitcoin - while Bitcoin remains larger by market cap, many see Ethereum as having broader use cases through its programmable nature.
    
    Some users have mentioned that while gas fees can still be high during peak usage, layer 2 solutions have been helping to address scalability concerns.
    
    Would you like to know more about any specific aspect of Ethereum?
    """
            # Update lineage metadata
            lineage.get_node(response_id).metadata.update({
                "model": "mock",
                "response_length": len(mock_response),
                "is_mock": True
            })
            
            return mock_response
        
        # Prepare the context
        context_parts = []
        
        # Add structured data to the context
        if structured_data:
            for coin in structured_data:
                context_parts.append(f"""
    Market Data for {coin['name']} ({coin['symbol']}):
    - Current Price: ${coin['price']:,.2f}
    - Market Cap: ${coin['market_cap']:,.2f}
    - 24h Trading Volume: ${coin['volume_24h']:,.2f}
    - Circulating Supply: {coin['circulating_supply']:,.0f} {coin['symbol']}
                """.strip())
        
        # Add Reddit posts to the context
        if reddit_posts:
            for i, post in enumerate(reddit_posts):
                context_parts.append(f"""
    Reddit Discussion {i+1}:
    Title: {post['title']}
    Content: {post['content']}
                """.strip())
        
        # Combine all context
        full_context = "\n\n".join(context_parts)
        
        if not full_context:
            # Update lineage metadata to show no context
            lineage.get_node(response_id).metadata.update({
                "error": "No context data available",
                "has_data": False
            })
            
            return f"I'm sorry, but I couldn't find any information about {query} in my database."
        
        # Build the prompt for the LLM
        system_prompt = """
    You are CryptoInsightBot, a helpful assistant specializing in cryptocurrency information.
    Your task is to create a conversational, helpful response based on the provided context information.
    You should synthesize the information and present it in a natural, informative way that directly answers the user's query.
    Include relevant market data and community discussions from the context, but paraphrase them in your own words.
    Make sure your response feels like a cohesive answer rather than just listing facts.
    If there are contradictions in the sources, acknowledge them and provide a balanced view.
    Keep your response concise yet informative, focusing on the most relevant details for the user's query.
    Do not mention "the context" or "the data" in your response - present the information as your own knowledge.
    Do not make up information that isn't in the provided context.
    """
        
        user_prompt = f"""
    User Query: {query}
    
    Context Information:
    {full_context}
    
    Please provide a helpful, conversational response to the user's query based on this context.
    """
        
        try:
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=500,
                temperature=0.7
            )
            
            generated_text = response.choices[0].message.content.strip()
            
            # Update lineage metadata
            lineage.get_node(response_id).metadata.update({
                "model": "gpt-3.5-turbo",
                "response_length": len(generated_text),
                "token_count": len(generated_text.split()),
                "has_data": True
            })
            
            return generated_text
        except Exception as e:
            logger.error(f"Error generating LLM response: {e}")
            
            # Update lineage metadata with error
            lineage.get_node(response_id).metadata.update({
                "error": str(e),
                "error_type": type(e).__name__
            })
            
            # Fallback to a simple response in case of API error
            if structured_data or reddit_posts:
                fallback = f"Based on my data, I found information about {query}. Here are some details: {full_context[:200]}..."
            else:
                fallback = f"I'm sorry, but I couldn't find any information about {query} in my database."
            return fallback

# Main function to handle the query and return a generated response
def chat(query, db_connection, posts_limit=3):
    """Process a user query and return a conversational response."""
    logger.info(f"Processing query: {query}")
    
    # Retrieve structured market data
    structured_data = retrieve_structured_data(query, db_connection)
    logger.info(f"Retrieved {len(structured_data)} structured data entries")
    
    # Retrieve relevant Reddit posts
    reddit_posts = retrieve_reddit_data(query, db_connection, top_k=posts_limit)
    logger.info(f"Retrieved {len(reddit_posts)} relevant Reddit posts")
    
    # Generate conversational response
    response = generate_conversational_response(query, structured_data, reddit_posts)
    logger.info("Generated conversational response")
    
    return response

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Improved RAG System for Crypto Data')
    parser.add_argument('--mock', action='store_true', help='Run in mock mode without real API calls')
    parser.add_argument('--query', type=str, default="Ethereum", help='Query to process')
    parser.add_argument('--posts', type=int, default=3, help='Number of Reddit posts to retrieve')
    args = parser.parse_args()
    
    if args.mock:
        enable_mock_mode()
    
    # Create a visualization directory
    os.makedirs('visualizations', exist_ok=True)
    
    # Load environment variables
    load_dotenv()
    
    try:
        # Create a data lineage pipeline node
        lineage = get_lineage_tracker()
        pipeline_id = lineage.add_node(
            node_type="transformation",
            name="RAG Pipeline",
            description="Retrieval-Augmented Generation pipeline for cryptocurrency queries",
            metadata={
                "query": args.query,
                "mock_mode": MOCK_MODE,
                "timestamp": datetime.now().isoformat()
            }
        )
        
        # Connect to the database
        db_connection = get_db_connection()
        
        # Get user query
        query = args.query
        
        # Get the chat response
        response = chat(query, db_connection, posts_limit=args.posts)
        
        # Generate lineage visualization
        lineage.visualize(output_file='visualizations/rag_lineage.html')
        lineage.export_json(output_file='visualizations/rag_lineage.json')
        
        # Display the response
        print("\n" + "="*50)
        print(f"Query: {query}")
        print("="*50)
        print(response)
        print("="*50 + "\n")
        
        logger.info("RAG pipeline completed successfully")
        logger.info(f"Data lineage visualization saved to visualizations/rag_lineage.html")
        
    except Exception as e:
        logger.error(f"RAG pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 