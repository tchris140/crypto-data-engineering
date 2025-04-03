"""
Comparison of different RAG implementations in the project.

This script compares:
1. Original RAG (RAG.py)
2. Improved RAG (improved_RAG.py)
3. LangChain RAG (langchain_rag.py)

Usage:
    python compare_rag_implementations.py --query "Ethereum" [--mock]
"""

import argparse
import logging
import time
from tabulate import tabulate

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_original_rag(query, mock_mode=False):
    """Test the original RAG implementation"""
    try:
        # Import the RAG module
        import RAG
        
        logger.info("Testing original RAG implementation...")
        start_time = time.time()
        
        # Process query
        if mock_mode:
            response = "This is a mock response from the original RAG. It provides basic information about Ethereum, the second-largest cryptocurrency by market cap."
        else:
            response = RAG.process_query(query)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        return {
            "execution_time": execution_time,
            "response": response
        }
    except Exception as e:
        logger.error(f"Error testing original RAG: {e}")
        return {
            "execution_time": 0,
            "response": f"Error: {e}"
        }

def test_improved_rag(query, mock_mode=False):
    """Test the improved RAG implementation"""
    try:
        # Import the improved_RAG module
        import improved_RAG
        
        logger.info("Testing improved RAG implementation...")
        start_time = time.time()
        
        # Process query
        if mock_mode:
            response = "This is a mock response from the improved RAG. It provides more detailed information about Ethereum, including its price, market cap, and recent community discussions about upgrades and DeFi applications."
        else:
            rag = improved_RAG.CryptoRAG(mock_mode=mock_mode)
            response = rag.answer_query(query)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        return {
            "execution_time": execution_time,
            "response": response
        }
    except Exception as e:
        logger.error(f"Error testing improved RAG: {e}")
        return {
            "execution_time": 0,
            "response": f"Error: {e}"
        }

def test_langchain_rag(query, mock_mode=False):
    """Test the LangChain RAG implementation"""
    try:
        # Import the LangChain RAG module
        from langchain_rag import CryptoRAGSystem
        
        logger.info("Testing LangChain RAG implementation...")
        start_time = time.time()
        
        # Process query
        langchain_rag = CryptoRAGSystem(mock_mode=mock_mode)
        response = langchain_rag.chat(query)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        return {
            "execution_time": execution_time,
            "response": response
        }
    except Exception as e:
        logger.error(f"Error testing LangChain RAG: {e}")
        return {
            "execution_time": 0,
            "response": f"Error: {e}"
        }

def compare_implementations(query, mock_mode=False, full_output=False):
    """Compare all three RAG implementations"""
    logger.info(f"Comparing RAG implementations with query: '{query}'")
    
    # Test each implementation
    original_result = test_original_rag(query, mock_mode)
    improved_result = test_improved_rag(query, mock_mode)
    langchain_result = test_langchain_rag(query, mock_mode)
    
    # Create a comparison table
    comparison = [
        ["Original RAG", f"{original_result['execution_time']:.2f}s", 
         original_result['response'] if full_output else original_result['response'][:100] + "..."],
        ["Improved RAG", f"{improved_result['execution_time']:.2f}s", 
         improved_result['response'] if full_output else improved_result['response'][:100] + "..."],
        ["LangChain RAG", f"{langchain_result['execution_time']:.2f}s", 
         langchain_result['response'] if full_output else langchain_result['response'][:100] + "..."]
    ]
    
    # Format the table
    table = tabulate(comparison, headers=["Implementation", "Execution Time", "Response"], tablefmt="grid")
    
    # Output the table
    logger.info("\nComparison Results:\n" + table)
    print("\n" + table)
    
    # Return the results
    return {
        "original": original_result,
        "improved": improved_result,
        "langchain": langchain_result
    }

def main():
    """Main function to run the comparison"""
    parser = argparse.ArgumentParser(description="Compare different RAG implementations")
    parser.add_argument("--query", type=str, default="Ethereum", help="Query to test with all implementations")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode without real API calls")
    parser.add_argument("--full", action="store_true", help="Show full response output instead of truncated")
    args = parser.parse_args()
    
    # Run the comparison
    compare_implementations(args.query, args.mock, args.full)
    
    logger.info("Comparison complete!")

if __name__ == "__main__":
    main() 