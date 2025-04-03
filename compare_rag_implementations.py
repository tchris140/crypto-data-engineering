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

# Import the three implementations
import RAG
import improved_RAG
from langchain_rag import CryptoRAGSystem

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def compare_implementations(query, mock_mode=False):
    """
    Compare the three RAG implementations with the same query.
    
    Args:
        query: Query to test with all implementations
        mock_mode: Whether to use mock mode (no API calls)
    
    Returns:
        Dictionary of results with times and responses
    """
    results = {}
    
    # 1. Original RAG
    logger.info("Testing original RAG implementation...")
    if mock_mode:
        RAG.enable_mock_mode()
    
    start_time = time.time()
    db_connection_original = RAG.get_db_connection()
    response_original = RAG.chat(query, db_connection_original)
    end_time = time.time()
    
    results["original"] = {
        "time": end_time - start_time,
        "response": response_original
    }
    
    logger.info(f"Original RAG completed in {results['original']['time']:.2f} seconds")
    
    # 2. Improved RAG
    logger.info("Testing improved RAG implementation...")
    if mock_mode:
        improved_RAG.enable_mock_mode()
    
    start_time = time.time()
    db_connection_improved = improved_RAG.get_db_connection()
    response_improved = improved_RAG.chat(query, db_connection_improved)
    end_time = time.time()
    
    results["improved"] = {
        "time": end_time - start_time,
        "response": response_improved
    }
    
    logger.info(f"Improved RAG completed in {results['improved']['time']:.2f} seconds")
    
    # 3. LangChain RAG
    logger.info("Testing LangChain RAG implementation...")
    
    start_time = time.time()
    langchain_rag = CryptoRAGSystem(mock_mode=mock_mode)
    response_langchain = langchain_rag.chat(query)
    end_time = time.time()
    
    results["langchain"] = {
        "time": end_time - start_time,
        "response": response_langchain
    }
    
    logger.info(f"LangChain RAG completed in {results['langchain']['time']:.2f} seconds")
    
    return results

def display_comparison_table(results):
    """
    Display a comparison table of the results.
    
    Args:
        results: Dictionary of results from compare_implementations
    """
    table_data = []
    
    for implementation, data in results.items():
        # Truncate response if too long
        response = data["response"]
        if len(response) > 100:
            response = response[:97] + "..."
            
        table_data.append([
            implementation.capitalize(),
            f"{data['time']:.2f}s",
            response
        ])
    
    # Print the table
    headers = ["Implementation", "Time", "Response"]
    print("\n" + tabulate(table_data, headers=headers, tablefmt="grid"))

def display_detailed_responses(results):
    """
    Display the full responses from each implementation.
    
    Args:
        results: Dictionary of results from compare_implementations
    """
    for implementation, data in results.items():
        print(f"\n{'=' * 40}")
        print(f" {implementation.upper()} RAG RESPONSE ")
        print(f"{'=' * 40}")
        print(data["response"])
        print(f"{'=' * 80}")

def main():
    """Main function to run the comparison"""
    parser = argparse.ArgumentParser(description='Compare RAG Implementations')
    parser.add_argument('--query', type=str, default="Ethereum", help='Query to process with all implementations')
    parser.add_argument('--mock', action='store_true', help='Run in mock mode without real API calls')
    parser.add_argument('--full', action='store_true', help='Display full responses')
    args = parser.parse_args()
    
    # Run the comparison
    results = compare_implementations(args.query, args.mock)
    
    # Display the results
    display_comparison_table(results)
    
    # Display full responses if requested
    if args.full:
        display_detailed_responses(results)
    
    logger.info("Comparison complete!")

if __name__ == "__main__":
    main() 