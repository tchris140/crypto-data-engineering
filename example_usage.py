"""
Example usage of the LangChain RAG implementation.

This script demonstrates how to use the LangChain RAG implementation programmatically.
"""

import argparse
from langchain_rag import CryptoRAGSystem

def interactive_mode(rag_system):
    """Run the LangChain RAG system in interactive mode"""
    print("\n========== Crypto RAG System with LangChain ==========")
    print("Type 'exit' or 'quit' to end the session.")
    print("Type 'help' for a list of available commands.")
    print("======================================================\n")
    
    while True:
        # Get the user's query
        query = input("\nWhat would you like to know about cryptocurrencies? > ")
        
        if query.lower() in ['exit', 'quit']:
            print("Goodbye!")
            break
        elif query.lower() == 'help':
            print("\nAvailable commands:")
            print("  help - Show this help message")
            print("  exit/quit - End the session")
            print("  <anything else> - Ask a question about cryptocurrencies")
            continue
        
        # Process the query
        response = rag_system.chat(query)
        
        # Display the response
        print("\n" + "="*50)
        print(response)
        print("="*50)

def batch_mode(rag_system, queries=None):
    """Process a list of queries in batch mode"""
    if not queries:
        queries = [
            "What is the current price of Bitcoin?",
            "How does Ethereum compare to Solana?",
            "What are people saying about cryptocurrency regulations?",
            "Is mining Bitcoin still profitable?",
            "What are the latest NFT trends?"
        ]
    
    print(f"\n{'='*50}")
    print(f"Processing {len(queries)} queries in batch mode:")
    print(f"{'='*50}\n")
    
    for i, query in enumerate(queries, 1):
        print(f"\nQuery {i}: {query}")
        print(f"{'-'*50}")
        response = rag_system.chat(query)
        print(f"\nResponse: {response}")
        print(f"\n{'='*50}")

def main():
    """Main function to run the example"""
    parser = argparse.ArgumentParser(description='Example usage of LangChain RAG')
    parser.add_argument('--query', type=str, help='Single query to process')
    parser.add_argument('--batch', action='store_true', help='Run in batch mode with predefined queries')
    parser.add_argument('--interactive', action='store_true', help='Run in interactive mode')
    parser.add_argument('--mock', action='store_true', help='Use mock mode instead of real API calls', default=True)
    
    args = parser.parse_args()
    
    # Initialize the RAG system
    rag_system = CryptoRAGSystem(mock_mode=args.mock)
    
    if args.query:
        # Process a single query
        response = rag_system.chat(args.query)
        print("\n" + "="*50)
        print(response)
        print("="*50)
    elif args.batch:
        # Run in batch mode with predefined queries
        batch_mode(rag_system)
    elif args.interactive or not (args.query or args.batch):
        # Run in interactive mode (default)
        interactive_mode(rag_system)

if __name__ == "__main__":
    main() 