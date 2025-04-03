import argparse
import logging
import pandas as pd
from datetime import datetime
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def generate_mock_data():
    print("Generating mock data...")
    # Generate some mock cryptocurrency data
    mock_data = [
        {"Name": "Ethereum", "Symbol": "ETH", "TVL": 50000000000, "Price (USD)": 3500.45, 
         "Market Cap (USD)": 420000000000, "24h Volume (USD)": 15000000000, 
         "Circulating Supply": 120000000, "Last Updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
        
        {"Name": "Binance", "Symbol": "BNB", "TVL": 20000000000, "Price (USD)": 600.75, 
         "Market Cap (USD)": 93000000000, "24h Volume (USD)": 3500000000, 
         "Circulating Supply": 155000000, "Last Updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
        
        {"Name": "Polygon", "Symbol": "MATIC", "TVL": 5000000000, "Price (USD)": 1.15, 
         "Market Cap (USD)": 10500000000, "24h Volume (USD)": 750000000, 
         "Circulating Supply": 9200000000, "Last Updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
        
        {"Name": "Arbitrum", "Symbol": "ARB", "TVL": 3000000000, "Price (USD)": 1.25, 
         "Market Cap (USD)": 3950000000, "24h Volume (USD)": 450000000, 
         "Circulating Supply": 3150000000, "Last Updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
        
        {"Name": "Solana", "Symbol": "SOL", "TVL": 1000000000, "Price (USD)": 145.32, 
         "Market Cap (USD)": 62500000000, "24h Volume (USD)": 3200000000, 
         "Circulating Supply": 430000000, "Last Updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    ]
    print(f"Generated {len(mock_data)} mock entries")
    return mock_data

def main():
    print("Starting DeFi Llama Mock script...")
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='DeFi Llama Scraper')
    parser.add_argument('--mock', action='store_true', help='Run in mock mode without real API calls')
    args = parser.parse_args()
    
    print(f"Mock mode enabled: {args.mock}")
    
    if args.mock:
        print("Mock mode enabled - no real API calls will be made")
        logger.info("Mock mode enabled - no real API calls will be made")
        
        mock_data = generate_mock_data()
        
        # Create DataFrame and save to CSV
        df = pd.DataFrame(mock_data)
        output_file = "output_data.csv"
        df.to_csv(output_file, index=False)
        print(f"Saved data to {output_file}")
        
        # Log processed data
        for entry in mock_data:
            print(f"Processing {entry['Name']} ({entry['Symbol']})")
            logger.info(f"{entry['Name']} ({entry['Symbol']}) - TVL: ${entry['TVL']:,.2f}")
            logger.info(f"   Price: ${entry['Price (USD)']:,.2f}")
            logger.info(f"   Market Cap: ${entry['Market Cap (USD)']:,.2f}")
            logger.info(f"   24h Volume: ${entry['24h Volume (USD)']:,.2f}")
            logger.info(f"   Circulating Supply: {entry['Circulating Supply']:,}")
            logger.info("-" * 50)
        
        logger.info(f"Data successfully saved to {output_file}")
        logger.info(f"Total coins processed: {len(df)}")
        logger.info(f"Columns: {', '.join(df.columns)}")
        print("Script completed successfully")
    else:
        print("Please run with --mock flag to use mock mode")
        logger.info("Please run with --mock flag to use mock mode")
        logger.error("The regular scraper mode has indentation issues. Use mock mode for testing.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Process interrupted by user")
        logger.info("Process interrupted by user")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        logger.exception(f"An unexpected error occurred: {e}") 