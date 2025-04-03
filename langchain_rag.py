"""
Enhanced RAG System using LangChain framework.

This implementation provides a more modular, extensible approach to the RAG pipeline,
leveraging LangChain for document loading, embedding, retrieval, and response generation.
It allows for easier swapping of components and more sophisticated prompt engineering.
"""

import argparse
import ast
import logging
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sklearn.metrics.pairwise import cosine_similarity

# LangChain imports - updated to recommended paths
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import PGVector
from langchain.schema import Document
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.chains import create_sql_query_chain
from langchain.chains import RetrievalQA
from langchain.schema.runnable import RunnablePassthrough
from langchain_text_splitters import RecursiveCharacterTextSplitter

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
openai_api_key = os.getenv('OPENAI_API_KEY')

# Configure connection string for pgvector
CONNECTION_STRING = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

class CryptoRAGSystem:
    """
    Enhanced RAG system using LangChain to integrate structured and unstructured crypto data.
    
    Features:
    - LangChain's Document and embedding models for vector storage and retrieval
    - Structured SQL queries for market data
    - Custom prompt engineering for response generation
    - Lineage tracking for data provenance
    """
    
    def __init__(self, mock_mode=False):
        """Initialize the RAG system with necessary components"""
        global MOCK_MODE
        MOCK_MODE = mock_mode
        
        # Initialize lineage tracker
        self.lineage = get_lineage_tracker()
        
        if not MOCK_MODE:
            # Set up LangChain components
            self.embeddings = OpenAIEmbeddings(api_key=openai_api_key)
            self.llm = ChatOpenAI(
                temperature=0.7,
                model="gpt-3.5-turbo",
                api_key=openai_api_key
            )
            
            try:
                # Initialize vector store for Reddit content
                self.setup_vector_store()
            except Exception as e:
                logger.error(f"Error setting up vector store: {e}")
                logger.warning("Using fallback methods for retrieval")

    def setup_vector_store(self):
        """Set up the vector store connection using pgvector"""
        try:
            # Create vector store connection to pgvector
            self.vector_store = PGVector(
                collection_name="reddit_vectors",
                connection_string=CONNECTION_STRING,
                embedding_function=self.embeddings,
                use_jsonb=True  # Use JSONB to store metadata
            )
            logger.info("Successfully connected to pgvector store")
        except Exception as e:
            logger.error(f"Failed to connect to pgvector: {e}")
            raise
    
    def get_db_connection(self):
        """Create database connection with error handling and mock support."""
        if MOCK_MODE:
            logger.info("Using mock database connection")
            # Create a mock connection object that mimics psycopg2 connection
            class MockCursor:
                def execute(self, query, params=None):
                    logger.info(f"Mock executing query: {query}")
                    self.last_query = query
                    if params:
                        self.query_params = params
                    else:
                        self.query_params = None
                    return self
                    
                def fetchall(self):
                    logger.info("Mock fetching results")
                    if "reddit_embeddings" in self.last_query:
                        # Mock Reddit embeddings - match based on query params
                        query_term = ""
                        if hasattr(self, 'query_params') and self.query_params:
                            # Extract search term from the parameters
                            if isinstance(self.query_params, tuple) and len(self.query_params) > 0:
                                query_term = self.query_params[0].lower() if isinstance(self.query_params[0], str) else ""
                            
                        if "bitcoin" in query_term.lower():
                            # Bitcoin-related mock data
                            return [
                                ["mock1", "Bitcoin Discussion", "Bitcoin is the first and largest cryptocurrency by market cap.", "[" + ",".join(["0.1"] * 1536) + "]"],
                                ["mock2", "Bitcoin vs Ethereum", "Comparing the two biggest cryptocurrencies. Bitcoin is more of a store of value, while Ethereum offers smart contracts.", "[" + ",".join(["0.2"] * 1536) + "]"],
                                ["mock3", "Bitcoin's Price Movement", "Bitcoin has been showing strong support levels after the recent halving event.", "[" + ",".join(["0.3"] * 1536) + "]"]
                            ]
                        elif "solana" in query_term.lower() or "sol" in query_term.lower():
                            return [
                                ["mock1", "Solana Discussion", "Solana is a high-throughput blockchain platform with low fees.", "[" + ",".join(["0.1"] * 1536) + "]"],
                                ["mock2", "Bitcoin vs Solana", "Comparing the two biggest cryptocurrencies. Solana has more use cases with its high throughput and low fees.", "[" + ",".join(["0.2"] * 1536) + "]"],
                                ["mock3", "Ethereum's Recent Growth", "Ethereum has seen massive adoption recently due to its smart contract platform, but Solana has more use cases with its high throughput and low fees.", "[" + ",".join(["0.3"] * 1536) + "]"]
                            ]
                        else:
                            # Default Ethereum-related mock data
                            return [
                                ["mock1", "Ethereum Discussion", "Ethereum is a great blockchain platform with smart contracts.", "[" + ",".join(["0.1"] * 1536) + "]"],
                                ["mock2", "Bitcoin vs Ethereum", "Comparing the two biggest cryptocurrencies. Ethereum has more use cases with its smart contract platform, but Bitcoin remains the largest by market cap.", "[" + ",".join(["0.2"] * 1536) + "]"],
                                ["mock3", "Solana's Recent Growth", "Solana has seen massive adoption recently due to its high throughput and low fees.", "[" + ",".join(["0.3"] * 1536) + "]"]
                            ]
                    else:
                        # Mock structured coin data - match based on query params
                        if hasattr(self, 'query_params') and self.query_params:
                            search_term = ""
                            if isinstance(self.query_params, tuple) and len(self.query_params) >= 2:
                                search_term = self.query_params[0].lower()
                                
                            if "bitcoin" in search_term.lower() or "btc" in search_term.lower():
                                return [["Bitcoin", "BTC", 65000.25, 1250000000000, 30000000000, 19500000]]
                            elif "solana" in search_term.lower() or "sol" in search_term.lower():
                                return [["Solana", "SOL", 145.87, 68000000000, 5200000000, 465000000]]
                            else:
                                return [["Ethereum", "ETH", 3500.45, 420000000000, 15000000000, 120000000]]
                        else:
                            # Default to Ethereum if no parameters
                            return [["Ethereum", "ETH", 3500.45, 420000000000, 15000000000, 120000000]]
                
                def fetchone(self):
                    """Mock fetchone method to handle vector column check."""
                    if "vector" in self.last_query:
                        return [True]  # Pretend vector extension is installed
                    elif "count" in self.last_query.lower():
                        return [0]  # Return 0 for count queries
                    return [False]  # Default response
                        
                def __init__(self):
                    self.last_query = ""
                    self.query_params = None
                    
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
    
    def retrieve_reddit_data_with_langchain(self, query: str, top_k: int = 3) -> List[Document]:
        """
        Retrieve relevant Reddit posts using LangChain's retriever.
        
        Args:
            query: User query to find relevant posts
            top_k: Number of posts to retrieve
            
        Returns:
            List of LangChain Document objects containing the posts
        """
        # Create data lineage nodes
        query_node_id = self.lineage.add_node(
            node_type="dataset",
            name="User Query",
            description="User query for RAG system",
            metadata={"query": query, "timestamp": datetime.now().isoformat()}
        )
        
        with LineageContext(
            source_nodes=[query_node_id],
            operation="retrieve",
            target_name="Similar Reddit Posts",
            target_description="Reddit posts most similar to the query using LangChain",
            target_type="dataset",
            metadata={"top_k": top_k, "timestamp": datetime.now().isoformat()}
        ) as results_id:
            
            if MOCK_MODE:
                # Return mock documents in mock mode
                mock_docs = []
                
                if "bitcoin" in query.lower() or "btc" in query.lower():
                    # Bitcoin-related mock data
                    mock_docs = [
                        Document(page_content="Bitcoin is the first and largest cryptocurrency by market cap, often referred to as digital gold.", 
                                 metadata={"title": "Bitcoin Discussion", "post_id": "mock1", "similarity": 0.92}),
                        Document(page_content="Comparing Bitcoin and Ethereum. Bitcoin is more of a store of value while Ethereum offers smart contract capabilities.", 
                                 metadata={"title": "Bitcoin vs Ethereum", "post_id": "mock2", "similarity": 0.89}),
                        Document(page_content="Bitcoin has been showing strong support levels after the recent halving event with institutional adoption increasing.", 
                                 metadata={"title": "Bitcoin's Price Movement", "post_id": "mock3", "similarity": 0.78})
                    ]
                elif "solana" in query.lower() or "sol" in query.lower():
                    mock_docs = [
                        Document(page_content="Solana is a high-throughput blockchain platform with low fees.", 
                                 metadata={"title": "Solana Discussion", "post_id": "mock1", "similarity": 0.92}),
                        Document(page_content="Comparing Bitcoin and Solana. Solana has more use cases with its high throughput and low fees.", 
                                 metadata={"title": "Bitcoin vs Solana", "post_id": "mock2", "similarity": 0.89}),
                        Document(page_content="Ethereum has seen massive adoption recently due to its smart contract platform, but Solana has more use cases with its high throughput and low fees.", 
                                 metadata={"title": "Ethereum's Recent Growth", "post_id": "mock3", "similarity": 0.78})
                    ]
                else:
                    # Default Ethereum-related mock data 
                    mock_docs = [
                        Document(page_content="Ethereum is a great blockchain platform with smart contracts.", 
                                 metadata={"title": "Ethereum Discussion", "post_id": "mock1", "similarity": 0.92}),
                        Document(page_content="Comparing Bitcoin and Ethereum. Ethereum has more use cases with its smart contract platform, but Bitcoin remains the largest by market cap.", 
                                 metadata={"title": "Bitcoin vs Ethereum", "post_id": "mock2", "similarity": 0.89}),
                        Document(page_content="Solana has seen massive adoption recently due to its high throughput and low fees.", 
                                 metadata={"title": "Solana's Recent Growth", "post_id": "mock3", "similarity": 0.78})
                    ]
                
                # Update lineage metadata
                self.lineage.get_node(results_id).metadata.update({
                    "posts_found": len(mock_docs),
                    "search_method": "mock",
                    "framework": "langchain"
                })
                
                return mock_docs
            
            try:
                # Use the vector store to find similar documents
                docs = self.vector_store.similarity_search_with_score(query, k=top_k)
                
                # Convert to format needed for our application
                result_docs = []
                for doc, score in docs:
                    # Add similarity score to metadata
                    doc.metadata["similarity"] = float(score)
                    result_docs.append(doc)
                
                # Update lineage metadata
                self.lineage.get_node(results_id).metadata.update({
                    "posts_found": len(result_docs),
                    "max_similarity": max([doc.metadata.get("similarity", 0) for doc in result_docs]) if result_docs else 0,
                    "search_method": "pgvector",
                    "framework": "langchain"
                })
                
                return result_docs
                
            except Exception as e:
                logger.error(f"Error retrieving data with LangChain: {e}")
                logger.info("Falling back to legacy retrieval method")
                
                # Fall back to the legacy retrieval method
                return self.retrieve_reddit_data_fallback(query, top_k)
    
    def retrieve_reddit_data_fallback(self, query: str, top_k: int = 3) -> List[Document]:
        """Legacy fallback method to retrieve Reddit posts if LangChain retrieval fails"""
        db_connection = self.get_db_connection()
        cursor = db_connection.cursor()
        
        try:
            # Generate embeddings for the query
            if not MOCK_MODE:
                query_embedding = self.embeddings.embed_query(query)
            else:
                query_embedding = [0.1] * 1536
                
            # Query to fetch embeddings from the Reddit posts
            cursor.execute("SELECT post_id, title, text, embedding FROM reddit_embeddings")
            rows = cursor.fetchall()
            
            if not rows:
                return []
            
            # Process the embeddings and calculate similarity
            embeddings = np.array([np.array(ast.literal_eval(row[3])) for row in rows])
            similarities = cosine_similarity([query_embedding], embeddings)[0]
            
            # Find the most similar posts
            top_indices = similarities.argsort()[-top_k:][::-1]
            result_docs = []
            
            for idx in top_indices:
                post = rows[idx]
                # Convert to LangChain Document format
                doc = Document(
                    page_content=post[2],  # post text
                    metadata={
                        "post_id": post[0],
                        "title": post[1],
                        "similarity": float(similarities[idx])
                    }
                )
                result_docs.append(doc)
            
            return result_docs
            
        except Exception as e:
            logger.error(f"Error in fallback retrieval: {e}")
            return []
        finally:
            cursor.close()
            db_connection.close()
    
    def retrieve_structured_data(self, query: str) -> Dict[str, Any]:
        """
        Retrieve structured market data for cryptocurrencies matching the query.
        
        Args:
            query: User query to search for relevant coins
            
        Returns:
            Dictionary containing structured market data
        """
        # Create data lineage nodes
        query_node_id = self.lineage.add_node(
            node_type="dataset",
            name="Market Data Query",
            description="Query for structured cryptocurrency market data",
            metadata={"query": query, "timestamp": datetime.now().isoformat()}
        )
        
        with LineageContext(
            source_nodes=[query_node_id],
            operation="retrieve",
            target_name="Structured Market Data",
            target_description="Structured cryptocurrency market data from database",
            target_type="dataset",
            metadata={"timestamp": datetime.now().isoformat()}
        ) as results_id:
            
            # Connect to the database
            db_connection = self.get_db_connection()
            cursor = db_connection.cursor()
            
            try:
                # Define your query to fetch the relevant structured data
                cursor.execute("""
                    SELECT "Name", "Symbol", "Price (USD)", "Market Cap (USD)", "24h Volume (USD)", "Circulating Supply" 
                    FROM coin_data_structured
                    WHERE "Name" ILIKE %s OR "Symbol" ILIKE %s
                """, (f"%{query}%", f"%{query}%"))
                rows = cursor.fetchall()
                
                if not rows:
                    self.lineage.get_node(results_id).metadata.update({
                        "coins_found": 0,
                        "search_query": query
                    })
                    return {}
                
                # Format the structured data as a dictionary
                data = rows[0]  # Assuming we're looking for the first matching coin
                structured_data = {
                    "name": data[0],
                    "symbol": data[1],
                    "price": data[2],
                    "market_cap": data[3],
                    "volume_24h": data[4],
                    "circulating_supply": data[5]
                }
                
                # Update lineage metadata
                self.lineage.get_node(results_id).metadata.update({
                    "coins_found": len(rows),
                    "coin_retrieved": data[0],
                    "search_query": query
                })
                
                return structured_data
                
            except Exception as e:
                logger.error(f"Error retrieving structured data: {e}")
                return {}
            finally:
                cursor.close()
                db_connection.close()
    
    def generate_response_with_langchain(self, query: str, structured_data: Dict[str, Any], reddit_docs: List[Document]) -> str:
        """
        Generate a conversational response using LangChain based on retrieved data.
        
        Args:
            query: Original user query
            structured_data: Dictionary of structured market data
            reddit_docs: List of relevant Reddit posts as LangChain Document objects
            
        Returns:
            Generated response text
        """
        # Create data lineage nodes
        retrieval_node_id = self.lineage.add_node(
            node_type="dataset",
            name="Retrieved Data",
            description="Combined structured and unstructured data for response generation",
            metadata={
                "query": query,
                "structured_data_available": bool(structured_data),
                "reddit_posts_available": len(reddit_docs),
                "timestamp": datetime.now().isoformat()
            }
        )
        
        with LineageContext(
            source_nodes=[retrieval_node_id],
            operation="generate",
            target_name="AI Response",
            target_description="Generated response using LangChain",
            target_type="dataset",
            metadata={"timestamp": datetime.now().isoformat()}
        ) as response_id:
            
            if MOCK_MODE:
                # Generate a mock response based on the cryptocurrency being queried
                crypto_name = structured_data.get('name', '').lower() if structured_data else ''
                
                if "bitcoin" in crypto_name or "btc" in crypto_name:
                    mock_response = (
                        f"Based on current market data, {structured_data.get('name', 'Bitcoin')} "
                        f"({structured_data.get('symbol', 'BTC')}) is trading at ${structured_data.get('price', 0):,.2f}. "
                        f"The market cap is ${structured_data.get('market_cap', 0):,.2f} with a 24-hour trading volume "
                        f"of ${structured_data.get('volume_24h', 0):,.2f}.\n\n"
                        f"According to recent Reddit discussions, Bitcoin is viewed as digital gold and a store of value. "
                        f"There has been increased institutional adoption, and the recent halving event has generated positive sentiment. "
                        f"Users are also discussing Bitcoin's performance relative to other cryptocurrencies like Ethereum."
                    )
                elif "solana" in crypto_name or "sol" in crypto_name:
                    mock_response = (
                        f"Based on current market data, {structured_data.get('name', 'Solana')} "
                        f"({structured_data.get('symbol', 'SOL')}) is trading at ${structured_data.get('price', 0):,.2f}. "
                        f"The market cap is ${structured_data.get('market_cap', 0):,.2f} with a 24-hour trading volume "
                        f"of ${structured_data.get('volume_24h', 0):,.2f}.\n\n"
                        f"According to recent Reddit discussions, Solana is viewed as a high-throughput blockchain platform with low fees. "
                        f"Users are also discussing Solana's performance relative to other cryptocurrencies like Ethereum."
                    )
                elif "regulat" in query.lower() or "compliance" in query.lower() or "law" in query.lower():
                    # Special case for regulation-related queries
                    mock_response = (
                        "Based on recent Reddit discussions about cryptocurrency regulations:\n\n"
                        "1. Many users are concerned about the evolving regulatory landscape across different countries.\n\n"
                        "2. There's significant discussion about the SEC's approach to cryptocurrency in the United States, "
                        "particularly regarding which tokens might be classified as securities.\n\n"
                        "3. Community sentiment suggests that clear regulations could help with institutional adoption, "
                        "though there are concerns about potential restrictions on innovation.\n\n"
                        "4. Several posts highlight that regulatory clarity varies significantly by country, with some nations "
                        "like Singapore and Switzerland establishing more comprehensive frameworks."
                    )
                elif "nft" in query.lower() or "non-fungible token" in query.lower():
                    # Special case for NFT-related queries
                    mock_response = (
                        "Based on recent Reddit discussions about NFTs and current market trends:\n\n"
                        "1. The NFT market has matured significantly since the 2021 boom, with more focus on utility and long-term value.\n\n"
                        "2. Gaming and metaverse-related NFTs continue to gain traction, with several major gaming companies launching NFT integrations.\n\n"
                        "3. There's growing interest in music NFTs and tokenized royalties, allowing artists to directly connect with fans.\n\n"
                        "4. Environmental concerns regarding NFT minting have led to more collections moving to energy-efficient blockchains like Polygon, Solana, and Ethereum post-merge.\n\n"
                        "5. Community sentiment indicates that the 'profile picture' NFT projects are seeing less speculative interest, while projects offering tangible utility or community benefits are showing more stability."
                    )
                elif "mining" in query.lower() or "miner" in query.lower():
                    # Special case for mining-related queries
                    mock_response = (
                        "Based on recent Reddit discussions about cryptocurrency mining:\n\n"
                        "1. Bitcoin mining profitability is highly dependent on electricity costs, with most profitable operations located in regions with low energy costs.\n\n"
                        "2. Following the April 2024 halving event, miners with older equipment have faced increased pressure as rewards were cut in half.\n\n"
                        "3. Many small-scale miners have shifted to alternative cryptocurrencies or joined mining pools to maintain profitability.\n\n"
                        "4. There's significant discussion about the environmental impact of mining, with a growing focus on renewable energy sources.\n\n"
                        "5. Community sentiment suggests that while mining is less accessible to individuals than in previous years, it remains viable for those with efficient operations and access to cheap electricity."
                    )
                else:
                    mock_response = (
                        f"Based on current market data, {structured_data.get('name', 'the cryptocurrency')} "
                        f"({structured_data.get('symbol', '')}) is trading at ${structured_data.get('price', 0):,.2f}. "
                        f"The market cap is ${structured_data.get('market_cap', 0):,.2f} with a 24-hour trading volume "
                        f"of ${structured_data.get('volume_24h', 0):,.2f}.\n\n"
                        f"According to recent Reddit discussions, users are talking about: "
                        f"{reddit_docs[0].page_content if reddit_docs else 'No relevant discussions found.'}"
                    )
                
                return mock_response
            
            # Define the system prompt
            system_template = """
            You are CryptoInsight, an expert cryptocurrency assistant that provides valuable insights based on market data and community discussions.
            
            Use the structured market data provided to give accurate information about cryptocurrency prices, market capitalization, and trading volumes.
            
            Also, incorporate insights from relevant Reddit discussions to provide context and community sentiment.
            
            Keep your responses concise, fact-based, and focused on the information provided in the structured data and Reddit posts.
            """
            
            # Format the structured data
            structured_content = ""
            if structured_data:
                structured_content = f"""
                MARKET DATA FOR {structured_data.get('name', '').upper()} ({structured_data.get('symbol', '')}):
                - Current Price: ${structured_data.get('price', 0):,.2f}
                - Market Cap: ${structured_data.get('market_cap', 0):,.2f}
                - 24h Trading Volume: ${structured_data.get('volume_24h', 0):,.2f}
                - Circulating Supply: {structured_data.get('circulating_supply', 0):,.0f} {structured_data.get('symbol', '')}
                """
            
            # Format the Reddit discussions
            reddit_content = "RELEVANT REDDIT DISCUSSIONS:\n"
            if reddit_docs:
                for i, doc in enumerate(reddit_docs, 1):
                    reddit_content += f"{i}. {doc.metadata.get('title', 'Reddit Post')}: {doc.page_content}\n\n"
            else:
                reddit_content += "No relevant Reddit discussions found.\n"
            
            # Create the human message template
            human_template = """
            Answer the following query about cryptocurrencies:
            
            USER QUERY: {query}
            
            Use these sources to provide an accurate and helpful response:
            
            {structured_content}
            
            {reddit_content}
            
            Respond in a helpful, conversational tone. Provide specific facts and figures from the data when available.
            """
            
            # Create the chat prompt
            chat_prompt = ChatPromptTemplate.from_messages([
                ("system", system_template),
                ("human", human_template)
            ])
            
            # Create the response generation chain
            chain = chat_prompt | self.llm
            
            # Generate the response
            response = chain.invoke({
                "query": query,
                "structured_content": structured_content,
                "reddit_content": reddit_content
            })
            
            # Update lineage metadata
            self.lineage.get_node(response_id).metadata.update({
                "model": "gpt-3.5-turbo",
                "framework": "langchain",
                "response_length": len(response.content)
            })
            
            return response.content
    
    def chat(self, query: str, posts_limit: int = 3) -> str:
        """
        Main method to process a user query and generate a response.
        
        Args:
            query: User query about cryptocurrencies
            posts_limit: Maximum number of Reddit posts to retrieve
            
        Returns:
            Generated response text
        """
        logger.info(f"Processing query: {query}")
        
        # Retrieve structured market data
        structured_data = self.retrieve_structured_data(query)
        
        # Retrieve relevant Reddit posts
        reddit_docs = self.retrieve_reddit_data_with_langchain(query, posts_limit)
        
        # Generate response using LangChain
        response = self.generate_response_with_langchain(query, structured_data, reddit_docs)
        
        return response

def main():
    """Main function for running the enhanced RAG system from command line"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Enhanced RAG System using LangChain')
    parser.add_argument('--mock', action='store_true', help='Run in mock mode without real API calls')
    parser.add_argument('--query', type=str, default="Ethereum", help='Query to process')
    parser.add_argument('--posts', type=int, default=3, help='Number of Reddit posts to retrieve')
    args = parser.parse_args()
    
    # Initialize the RAG system
    rag_system = CryptoRAGSystem(mock_mode=args.mock)
    
    # Get the chat response
    response = rag_system.chat(args.query, posts_limit=args.posts)
    
    # Display the response
    print("\n" + "="*50)
    print(response)
    print("="*50 + "\n")

if __name__ == "__main__":
    main() 