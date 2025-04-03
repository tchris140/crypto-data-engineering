import requests
import time
import difflib
import os
import sys
import logging
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import json
import argparse

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

def enable_mock_mode():
    """Enable mock mode for testing without API calls"""
    global MOCK_MODE
    MOCK_MODE = True
    logger.info("Mock mode enabled - no real API calls will be made")

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

def get_api_key():
    """Get API key from environment variables with error handling."""
    api_key = os.getenv('cmc_api_key')
    if not api_key and not MOCK_MODE:
        logger.error("CoinMarketCap API key not found, set it as an environment variable.")
        raise ValueError("Missing API key")
    return api_key

def fetch_tvl_data(limit=100):
    """Fetch TVL data from DeFi Llama API with error handling.
    
    Args:
        limit: Maximum number of chains to fetch
        
    Returns:
        List of dictionaries containing TVL data
    """
    # Create a data lineage node for the source
    lineage = get_lineage_tracker()
    source_id = lineage.add_node(
        node_type="source",
        name="DeFi Llama API",
        description="API providing Total Value Locked data for DeFi protocols",
        metadata={"endpoint": "https://api.llama.fi/protocols", "limit": limit}
    )
    
    if MOCK_MODE:
        logger.info("Using mock data for DeFi Llama TVL")
        try:
            with open('mock_data/defillama_tvl.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("Mock data file not found, generating sample data")
            return [
                {"name": "Ethereum", "symbol": "ETH", "tvl": 100000000000},
                {"name": "Binance", "symbol": "BNB", "tvl": 50000000000},
                {"name": "Polygon", "symbol": "MATIC", "tvl": 5000000000}
            ]
    
    url = "https://api.llama.fi/protocols"
    
    try:
        logger.info(f"Fetching TVL data from DeFi Llama API: {url}")
        
        with LineageContext(
            source_nodes=source_id,
            operation="extract",
            target_name="DeFi Llama TVL Data",
            target_description="Raw TVL data from DeFi Llama API",
            metadata={"url": url, "timestamp": datetime.now().isoformat()}
        ) as raw_data_id:
            
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched {len(data)} protocols from DeFi Llama")
            
            # Add metadata to the lineage
            lineage.get_node(raw_data_id).metadata.update({
                "record_count": len(data),
                "first_protocol": data[0]["name"] if data else None
            })
            
            return data[:limit]
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching TVL data: {e}")
        raise

def fetch_market_data(symbols, api_key):
    """Fetch market data from CoinMarketCap API with error handling.
    
    Args:
        symbols: List of cryptocurrency symbols to fetch
        api_key: CoinMarketCap API key
        
    Returns:
        Dictionary containing market data
    """
    # Create a data lineage node for the source
    lineage = get_lineage_tracker()
    source_id = lineage.add_node(
        node_type="source",
        name="CoinMarketCap API",
        description="API providing cryptocurrency market data",
        metadata={"symbols": symbols}
    )
    
    if MOCK_MODE:
        logger.info("Using mock data for CoinMarketCap")
        try:
            with open('mock_data/coinmarketcap.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("Mock data file not found, generating sample data")
            mock_data = {}
            for symbol in symbols:
                mock_data[symbol] = {
                    "price": 1000.0,
                    "volume_24h": 1000000.0,
                    "market_cap": 10000000000.0,
                    "circulating_supply": 10000000.0
                }
            return mock_data
    
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
    
    try:
        logger.info(f"Fetching market data from CoinMarketCap API for {len(symbols)} symbols")
        
        with LineageContext(
            source_nodes=source_id,
            operation="extract",
            target_name="CoinMarketCap Data",
            target_description="Raw market data from CoinMarketCap API",
            metadata={"url": url, "timestamp": datetime.now().isoformat()}
        ) as raw_data_id:
            
            headers = {
                'X-CMC_PRO_API_KEY': api_key,
                'Accept': 'application/json'
            }
            
            # Join symbols with commas for the API request
            symbol_string = ','.join(symbols)
            
            params = {
                'symbol': symbol_string
            }
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched market data for {len(symbols)} symbols")
            
            result = {}
            for symbol in symbols:
                try:
                    coin_data = data['data'][symbol]
                    result[symbol] = {
                        'price': coin_data['quote']['USD']['price'],
                        'volume_24h': coin_data['quote']['USD']['volume_24h'],
                        'market_cap': coin_data['quote']['USD']['market_cap'],
                        'circulating_supply': coin_data['circulating_supply']
                    }
                except KeyError:
                    logger.warning(f"Symbol {symbol} not found in CoinMarketCap response")
                    result[symbol] = {
                        'price': 0.0,
                        'volume_24h': 0.0,
                        'market_cap': 0.0,
                        'circulating_supply': 0.0
                    }
                    
            # Add metadata to the lineage
            lineage.get_node(raw_data_id).metadata.update({
                "symbols_requested": len(symbols),
                "symbols_found": len(result)
            })
            
            return result
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching market data: {e}")
        raise

def process_data(tvl_data, market_data):
    """Process and combine TVL and market data with error handling.
    
    Args:
        tvl_data: List of dictionaries containing TVL data
        market_data: Dictionary containing market data
        
    Returns:
        Pandas DataFrame with processed data
    """
    # Create data lineage nodes for the input datasets
    lineage = get_lineage_tracker()
    tvl_dataset_id = lineage.add_node(
        node_type="dataset",
        name="TVL Dataset",
        description="Processed TVL data",
        metadata={"record_count": len(tvl_data)}
    )
    
    market_dataset_id = lineage.add_node(
        node_type="dataset",
        name="Market Dataset",
        description="Processed market data",
        metadata={"record_count": len(market_data)}
    )
    
    try:
        logger.info("Processing and combining TVL and market data")
        
        with LineageContext(
            source_nodes=[tvl_dataset_id, market_dataset_id],
            operation="transform",
            target_name="Combined Crypto Dataset",
            target_description="Processed and combined TVL and market data",
            target_type="dataset",
            metadata={"timestamp": datetime.now().isoformat()}
        ) as combined_data_id:
            
            # Create a list to store the processed data
            processed_data = []
            
            for protocol in tvl_data:
                try:
                    name = protocol.get('name', 'Unknown')
                    symbol = protocol.get('symbol', 'UNKNOWN')
                    
                    # Get market data for this symbol if available
                    market_info = market_data.get(symbol, {
                        'price': 0.0,
                        'volume_24h': 0.0,
                        'market_cap': 0.0,
                        'circulating_supply': 0.0
                    })
                    
                    processed_data.append({
                        'Name': name,
                        'Symbol': symbol,
                        'TVL (USD)': protocol.get('tvl', 0.0),
                        'Price (USD)': market_info.get('price', 0.0),
                        'Market Cap (USD)': market_info.get('market_cap', 0.0),
                        '24h Volume (USD)': market_info.get('volume_24h', 0.0),
                        'Circulating Supply': market_info.get('circulating_supply', 0.0)
                    })
                except Exception as e:
                    logger.warning(f"Error processing protocol {protocol.get('name', 'Unknown')}: {e}")
            
            df = pd.DataFrame(processed_data)
            
            # Add metadata to the lineage
            lineage.get_node(combined_data_id).metadata.update({
                "record_count": len(df),
                "columns": df.columns.tolist()
            })
            
            return df
        
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise

def save_to_csv(data, output_file='output_data.csv'):
    """Save processed data to CSV with error handling.
    
    Args:
        data: Pandas DataFrame with processed data
        output_file: Path to the output CSV file
    """
    # Create a data lineage node for the input dataset
    lineage = get_lineage_tracker()
    data_id = lineage.add_node(
        node_type="dataset",
        name="Processed Dataset",
        description="Final processed dataset ready for saving",
        metadata={"record_count": len(data), "columns": data.columns.tolist()}
    )
    
    try:
        logger.info(f"Saving data to {output_file}")
        
        with LineageContext(
            source_nodes=data_id,
            operation="store",
            target_name="CSV File",
            target_description=f"CSV file at {output_file}",
            target_type="destination",
            metadata={"file_path": output_file, "timestamp": datetime.now().isoformat()}
        ):
            data.to_csv(output_file, index=False)
            logger.info(f"Successfully saved {len(data)} records to {output_file}")
        
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        raise

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Fetch and process crypto data')
    parser.add_argument('--mock', action='store_true', help='Run in mock mode without real API calls')
    parser.add_argument('--limit', type=int, default=100, help='Maximum number of protocols to fetch')
    parser.add_argument('--output', type=str, default='output_data.csv', help='Output file path')
    
    return parser.parse_args()

def main():
    """Main function orchestrating the data pipeline."""
    # Parse command line arguments
    args = parse_args()
    
    # Enable mock mode if specified
    if args.mock:
        enable_mock_mode()
    
    # Load environment variables
    load_dotenv()
    
    # Create a visualization directory
    os.makedirs('visualizations', exist_ok=True)
    
    try:
        # Fetch data
        tvl_data = fetch_tvl_data(limit=args.limit)
        
        # Get unique symbols for market data
        symbols = list(set(protocol['symbol'] for protocol in tvl_data if 'symbol' in protocol))
        
        # Fetch market data
        api_key = get_api_key()
        market_data = fetch_market_data(symbols, api_key)
        
        # Process data
        processed_data = process_data(tvl_data, market_data)
        
        # Save to CSV
        save_to_csv(processed_data, output_file=args.output)
        
        # Generate lineage visualization
        lineage = get_lineage_tracker()
        lineage.visualize(output_file='visualizations/defillama_lineage.html')
        lineage.export_json(output_file='visualizations/defillama_lineage.json')
        
        logger.info("Data pipeline completed successfully")
        logger.info(f"Data lineage visualization saved to visualizations/defillama_lineage.html")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()