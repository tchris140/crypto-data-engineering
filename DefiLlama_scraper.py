import requests
import time
import difflib
import os
import sys
import logging
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# API Keys and Endpoints
DEFI_LLAMA_URL = "https://api.llama.fi/chains"
CMC_API_KEY = os.getenv('cmc_api_key')
CMC_MAP_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
CMC_QUOTE_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

# Known manual name-to-symbol mappings (for mismatched names)
MANUAL_MAPPINGS = {
    "Ethereum": "ETH",
    "Binance": "BNB",
    "Polygon": "MATIC",
    "Arbitrum": "ARB",
    "Optimism": "OP",
    "Fantom": "FTM",
    "Avalanche": "AVAX",
    "Solana": "SOL",
    "Cronos": "CRO",
    "Celo": "CELO",
    "Moonbeam": "GLMR",
    "Moonriver": "MOVR",
    "RSK": "RBTC",
    "Harmony": "ONE",
    "Velas": "VLX",
    "Oasis": "ROSE",
}

# Excluded blockchain names
EXCLUDED_NAMES = {"Bahamut"}

def format_currency(value):
    """Format currency values for display"""
    return f"${value:,.2f}" if isinstance(value, (int, float)) else "N/A"

def get_cmc_coin_mapping():
    """Fetch coin mappings from CoinMarketCap"""
    logger.info("Fetching coin mappings from CoinMarketCap...")
    
    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    map_response = requests.get(CMC_MAP_URL, headers=headers)
    
    if map_response.status_code == 200:
        cmc_data = map_response.json()["data"]
        
        # Create a dictionary mapping names & slugs to symbols
        cmc_map = {coin["name"]: coin["symbol"] for coin in cmc_data}
        cmc_map.update({coin["slug"]: coin["symbol"] for coin in cmc_data})
        
        logger.info(f"Successfully fetched {len(cmc_map)} coin mappings")
        return cmc_map
    else:
        logger.error(f"Failed to fetch CoinMarketCap coin mapping. Status code: {map_response.status_code}")
        return {}

def get_top_chains_from_defi_llama(limit=50):
    """Fetch top TVL chains from DeFi Llama"""
    logger.info(f"Fetching top {limit} TVL chains from DeFi Llama...")
    
    response = requests.get(DEFI_LLAMA_URL)
    
    if response.status_code == 200:
        data = response.json()
        
        # Sort by TVL and filter out excluded names
        top_chains = sorted(
            [chain for chain in data if chain.get('name') not in EXCLUDED_NAMES], 
            key=lambda x: x.get('tvl', 0), 
            reverse=True
        )[:limit]  # Ensure we get the specified limit of valid entries
        
        logger.info(f"Successfully fetched {len(top_chains)} chains")
        return top_chains
    else:
        logger.error(f"Failed to fetch TVL data from DefiLlama. Status code: {response.status_code}")
        return []

def match_coin_symbol(name, cmc_map):
    """Match blockchain name to coin symbol using multiple strategies"""
    # Check manual mappings first
    if name in MANUAL_MAPPINGS:
        logger.debug(f"Found manual mapping for {name}: {MANUAL_MAPPINGS[name]}")
        return MANUAL_MAPPINGS[name]
    
    # Try exact match
    symbol = cmc_map.get(name)
    
    # If no exact match, try fuzzy matching
    if not symbol:
        possible_matches = difflib.get_close_matches(name, cmc_map.keys(), n=1, cutoff=0.7)
        if possible_matches:
            symbol = cmc_map.get(possible_matches[0])
            if symbol:
                logger.debug(f"Fuzzy matched {name} to {possible_matches[0]} with symbol {symbol}")
    
    return symbol

def fetch_market_data_batch(coin_batch, retries=3, delay=1):
    """Fetch market data for a batch of coins from CoinMarketCap"""
    symbols_query = ",".join([coin["symbol"] for coin in coin_batch])
    logger.info(f"Fetching market data for batch: {symbols_query}")
    
    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"symbol": symbols_query, "convert": "USD"}
    
    for attempt in range(retries):
        try:
            cmc_response = requests.get(CMC_QUOTE_URL, headers=headers, params=params)
            
            if cmc_response.status_code == 200:
                return cmc_response.json().get("data", {})
            elif cmc_response.status_code == 429:  # Rate limit error
                logger.warning(f"Rate limit hit. Retrying after delay. Attempt {attempt+1}/{retries}")
                time.sleep(delay * (2 ** attempt))  # Exponential backoff
            else:
                logger.error(f"Failed to fetch market data. Status Code: {cmc_response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Error fetching market data: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
                return {}
    
    logger.error(f"Failed to fetch market data for batch after {retries} attempts")
    return {}

def process_coin_data(coin_batch, market_data):
    """Process coin data with market information from CoinMarketCap"""
    processed_data = []
    
    for coin in coin_batch:
        symbol = coin["symbol"]
        name = coin["name"]
        tvl = coin["tvl"]
        
        if symbol in market_data:
            coin_data = market_data[symbol]["quote"]["USD"]
            price = coin_data.get("price", "N/A")
            market_cap = coin_data.get("market_cap", "N/A")
            volume_24h = coin_data.get("volume_24h", "N/A")
            circulating_supply = market_data[symbol].get("circulating_supply", "N/A")
        else:
            logger.warning(f"No market data found for {name} ({symbol})")
            price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'
        
        # Append data for DataFrame
        processed_data.append({
            "Name": name,
            "Symbol": symbol,
            "TVL": tvl,
            "Price (USD)": price,
            "Market Cap (USD)": market_cap,
            "24h Volume (USD)": volume_24h,
            "Circulating Supply": circulating_supply,
            "Last Updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Log the processed data
        logger.info(f"{name} ({symbol}) - TVL: {format_currency(tvl)}")
        logger.info(f"   Price: {format_currency(price)}")
        logger.info(f"   Market Cap: {format_currency(market_cap)}")
        logger.info(f"   24h Volume: {format_currency(volume_24h)}")
        logger.info(f"   Circulating Supply: {circulating_supply if isinstance(circulating_supply, (int, float)) else 'N/A'}")
        logger.info("-" * 50)
    
    return processed_data

def main():
    """Main function to orchestrate data collection and processing"""
    # Get coin name-to-symbol mappings
    cmc_map = get_cmc_coin_mapping()
    if not cmc_map:
        logger.error("Failed to get coin mappings. Exiting.")
        sys.exit(1)
    
    # Get top chains by TVL
    top_chains = get_top_chains_from_defi_llama()
    if not top_chains:
        logger.error("Failed to get chain data. Exiting.")
        sys.exit(1)
    
    # Collect coins with symbols
    coins = []
    missing_coins = []
    
    for blockchain in top_chains:
        name = blockchain.get('name', 'N/A')
        tvl = blockchain.get('tvl', 0)
        
        symbol = match_coin_symbol(name, cmc_map)
        
        if not symbol:
            logger.warning(f"No symbol found for {name}, skipping market data fetch...")
            missing_coins.append(name)
            continue
        
        coins.append({"name": name, "symbol": symbol, "tvl": tvl})
    
    # Batch process market data requests
    batch_size = 10
    all_processed_data = []
    
    for i in range(0, len(coins), batch_size):
        batch = coins[i:i + batch_size]
        
        # Fetch market data for batch
        market_data = fetch_market_data_batch(batch)
        
        # Process the data for each coin in the batch
        processed_batch = process_coin_data(batch, market_data)
        all_processed_data.extend(processed_batch)
        
        # Sleep to avoid hitting API limits
        if i + batch_size < len(coins):
            logger.info("Sleeping to respect API rate limits...")
            time.sleep(5)
    
    # Log missing coins
    if missing_coins:
        with open("missing_coins.txt", "w") as f:
            for coin in missing_coins:
                f.write(f"{coin}\n")
        logger.warning(f"Missing coins saved to 'missing_coins.txt' for manual review. Total: {len(missing_coins)}")
    
    # Create DataFrame and save to CSV
    if all_processed_data:
        df = pd.DataFrame(all_processed_data)
        output_file = "output_data.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Data successfully saved to {output_file}")
        
        # Display final data summary
        logger.info(f"Total coins processed: {len(df)}")
        logger.info(f"Columns: {', '.join(df.columns)}")
    else:
        logger.error("No data was processed. CSV file not created.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        sys.exit(1)