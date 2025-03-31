# # # # # # # # # import requests

# # # # # # # # # # DefiLlama API endpoint for blockchain TVL data
# # # # # # # # # url = "https://api.llama.fi/chains"

# # # # # # # # # # Send a GET request to the API
# # # # # # # # # response = requests.get(url)

# # # # # # # # # # Check if the request was successful
# # # # # # # # # if response.status_code == 200:
# # # # # # # # #     data = response.json()
# # # # # # # # #     # Sort the data by TVL in USD, descending order, and select the top 50
# # # # # # # # #     top_50_tvl = sorted(data, key=lambda x: x.get('tvlUsd', 0), reverse=True)[:50]
# # # # # # # # #     # Display the top 50 TVL blockchains
# # # # # # # # #     for i, blockchain in enumerate(top_50_tvl, 1):
# # # # # # # # #         name = blockchain.get('name', 'N/A')
# # # # # # # # #         tvl_usd = blockchain.get('tvlUsd', 0)
# # # # # # # # #         print(f"{i}. {name} - TVL: ${tvl_usd:,.2f}")
# # # # # # # # # else:
# # # # # # # # #     print(f"Failed to fetch data. HTTP Status code: {response.status_code}")



# # # # # # # # import requests
# # # # # # # # import json

# # # # # # # # # DefiLlama API endpoint for blockchain TVL data
# # # # # # # # url = "https://api.llama.fi/chains"

# # # # # # # # # Send a GET request to the API
# # # # # # # # response = requests.get(url)

# # # # # # # # # Check if the request was successful
# # # # # # # # if response.status_code == 200:
# # # # # # # #     data = response.json()
# # # # # # # #     # Print the entire response to inspect the data structure
# # # # # # # #     print(json.dumps(data, indent=4))
# # # # # # # # else:
# # # # # # # #     print(f"Failed to fetch data. HTTP Status code: {response.status_code}")


# # # # # # # import requests
# # # # # # # import json

# # # # # # # # DefiLlama API endpoint for blockchain TVL data
# # # # # # # url = "https://api.llama.fi/chains"

# # # # # # # # Send a GET request to the API
# # # # # # # response = requests.get(url)

# # # # # # # # Check if the request was successful
# # # # # # # if response.status_code == 200:
# # # # # # #     data = response.json()
# # # # # # #     # Sort the data by TVL in descending order and select the top 50
# # # # # # #     top_50_tvl = sorted(data, key=lambda x: x.get('tvl', 0), reverse=True)[:50]
# # # # # # #     # Display the top 50 TVL blockchains
# # # # # # #     for i, blockchain in enumerate(top_50_tvl, 1):
# # # # # # #         name = blockchain.get('name', 'N/A')
# # # # # # #         tvl = blockchain.get('tvl', 0)
# # # # # # #         print(f"{i}. {name} - TVL: ${tvl:,.2f}")
# # # # # # # else:
# # # # # # #     print(f"Failed to fetch data. HTTP Status code: {response.status_code}")


# # # # # # import requests
# # # # # # import json

# # # # # # # DefiLlama API endpoint for blockchain TVL data
# # # # # # defi_url = "https://api.llama.fi/chains"

# # # # # # # CoinGecko API endpoint for cryptocurrency market data
# # # # # # coingecko_url = "https://api.coingecko.com/api/v3/coins/"

# # # # # # # Send a GET request to DefiLlama API
# # # # # # response = requests.get(defi_url)

# # # # # # # Check if the request was successful
# # # # # # if response.status_code == 200:
# # # # # #     data = response.json()
# # # # # #     # Sort the data by TVL in descending order and select the top 50
# # # # # #     top_50_tvl = sorted(data, key=lambda x: x.get('tvl', 0), reverse=True)[:50]

# # # # # #     for i, blockchain in enumerate(top_50_tvl, 1):
# # # # # #         name = blockchain.get('name', 'N/A')
# # # # # #         tvl = blockchain.get('tvl', 0)
# # # # # #         gecko_id = blockchain.get('gecko_id', None)

# # # # # #         # Fetch market data from CoinGecko if gecko_id is available
# # # # # #         if gecko_id:
# # # # # #             market_data_response = requests.get(coingecko_url + gecko_id)
            
# # # # # #             if market_data_response.status_code == 200:
# # # # # #                 market_data = market_data_response.json()
# # # # # #                 price = market_data['market_data'].get('current_price', {}).get('usd', 'N/A')
# # # # # #                 market_cap = market_data['market_data'].get('market_cap', {}).get('usd', 'N/A')
# # # # # #                 volume_24h = market_data['market_data'].get('total_volume', {}).get('usd', 'N/A')
# # # # # #                 circulating_supply = market_data['market_data'].get('circulating_supply', 'N/A')
# # # # # #             else:
# # # # # #                 print(f"Failed to fetch market data for {name}")
# # # # # #                 price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'
# # # # # #         else:
# # # # # #             price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'

# # # # # #         # Display the top 50 TVL blockchains with market data
# # # # # #         print(f"{i}. {name} - TVL: ${tvl:,.2f}")
# # # # # #         print(f"   Price: ${price}")
# # # # # #         print(f"   Market Cap: ${market_cap}")
# # # # # #         print(f"   24h Trading Volume: ${volume_24h}")
# # # # # #         print(f"   Circulating Supply: {circulating_supply}")
# # # # # #         print("-" * 50)

# # # # # # else:
# # # # # #     print(f"Failed to fetch data from DefiLlama. HTTP Status code: {response.status_code}")



# # # # # import requests
# # # # # import json

# # # # # # DefiLlama API endpoint for blockchain TVL data
# # # # # defi_url = "https://api.llama.fi/chains"

# # # # # # CoinGecko API endpoint for cryptocurrency market data
# # # # # coingecko_url = "https://api.coingecko.com/api/v3/coins/"

# # # # # # Send a GET request to DefiLlama API
# # # # # response = requests.get(defi_url)

# # # # # # Check if the request was successful
# # # # # if response.status_code == 200:
# # # # #     data = response.json()
# # # # #     # Sort the data by TVL in descending order and select the top 50
# # # # #     top_50_tvl = sorted(data, key=lambda x: x.get('tvl', 0), reverse=True)[:50]

# # # # #     for i, blockchain in enumerate(top_50_tvl, 1):
# # # # #         name = blockchain.get('name', 'N/A')
# # # # #         tvl = blockchain.get('tvl', 0)
# # # # #         gecko_id = blockchain.get('gecko_id', None)

# # # # #         # Print out gecko_id to debug
# # # # #         print(f"Fetching data for {name} (Gecko ID: {gecko_id})")

# # # # #         # Fetch market data from CoinGecko if gecko_id is available
# # # # #         if gecko_id:
# # # # #             market_data_response = requests.get(coingecko_url + gecko_id)
            
# # # # #             # Print the full response to debug
# # # # #             print(f"Response from CoinGecko for {name}: {market_data_response.status_code}")
# # # # #             if market_data_response.status_code == 200:
# # # # #                 market_data = market_data_response.json()
# # # # #                 price = market_data['market_data'].get('current_price', {}).get('usd', 'N/A')
# # # # #                 market_cap = market_data['market_data'].get('market_cap', {}).get('usd', 'N/A')
# # # # #                 volume_24h = market_data['market_data'].get('total_volume', {}).get('usd', 'N/A')
# # # # #                 circulating_supply = market_data['market_data'].get('circulating_supply', 'N/A')
# # # # #             else:
# # # # #                 print(f"Failed to fetch market data for {name}, status code: {market_data_response.status_code}")
# # # # #                 price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'
# # # # #         else:
# # # # #             price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'

# # # # #         # Display the top 50 TVL blockchains with market data
# # # # #         print(f"{i}. {name} - TVL: ${tvl:,.2f}")
# # # # #         print(f"   Price: {price}")
# # # # #         print(f"   Market Cap: {market_cap}")
# # # # #         print(f"   24h Trading Volume: {volume_24h}")
# # # # #         print(f"   Circulating Supply: {circulating_supply}")
# # # # #         print("-" * 50)

# # # # # else:
# # # # #     print(f"Failed to fetch data from DefiLlama. HTTP Status code: {response.status_code}")


# # # # import requests
# # # # import json
# # # # import time

# # # # # DefiLlama API endpoint for blockchain TVL data
# # # # defi_url = "https://api.llama.fi/chains"

# # # # # CoinGecko API endpoint for cryptocurrency market data (search endpoint)
# # # # coingecko_search_url = "https://api.coingecko.com/api/v3/search?query="

# # # # # Send a GET request to DefiLlama API
# # # # response = requests.get(defi_url)

# # # # # Check if the request was successful
# # # # if response.status_code == 200:
# # # #     data = response.json()
# # # #     # Sort the data by TVL in descending order and select the top 50
# # # #     top_50_tvl = sorted(data, key=lambda x: x.get('tvl', 0), reverse=True)[:50]

# # # #     for i, blockchain in enumerate(top_50_tvl, 1):
# # # #         name = blockchain.get('name', 'N/A')
# # # #         tvl = blockchain.get('tvl', 0)

# # # #         # Print out the name to search for the gecko_id
# # # #         print(f"Fetching data for {name} (Searching on CoinGecko)...")

# # # #         # Search CoinGecko for the blockchain/cryptocurrency
# # # #         search_response = requests.get(coingecko_search_url + name)

# # # #         if search_response.status_code == 200:
# # # #             search_data = search_response.json()
# # # #             # Check if the search returned results and get the first match
# # # #             if search_data['coins']:
# # # #                 gecko_id = search_data['coins'][0]['id']
# # # #                 print(f"Found Gecko ID for {name}: {gecko_id}")

# # # #                 # Now fetch the market data using the gecko_id
# # # #                 market_data_response = requests.get(f"https://api.coingecko.com/api/v3/coins/{gecko_id}")
                
# # # #                 if market_data_response.status_code == 200:
# # # #                     market_data = market_data_response.json()
# # # #                     print(f"Market data for {name}: {json.dumps(market_data, indent=2)}")  # Debugging

# # # #                 if market_data_response.status_code == 200:
# # # #                     market_data = market_data_response.json()
# # # #                     price = market_data['market_data'].get('current_price', {}).get('usd', 'N/A')
# # # #                     market_cap = market_data['market_data'].get('market_cap', {}).get('usd', 'N/A')
# # # #                     volume_24h = market_data['market_data'].get('total_volume', {}).get('usd', 'N/A')
# # # #                     circulating_supply = market_data['market_data'].get('circulating_supply', 'N/A')
# # # #                 else:
# # # #                     print(f"Failed to fetch market data for {name}")
# # # #                     price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'
# # # #             else:
# # # #                 print(f"No results found for {name} on CoinGecko.")
# # # #                 price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'
# # # #         else:
# # # #             print(f"Failed to search on CoinGecko for {name}.")
# # # #             price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'

# # # #         # Display the top 50 TVL blockchains with market data
# # # #         print(f"{i}. {name} - TVL: ${tvl:,.2f}")
# # # #         print(f"   Price: {price}")
# # # #         print(f"   Market Cap: {market_cap}")
# # # #         print(f"   24h Trading Volume: {volume_24h}")
# # # #         print(f"   Circulating Supply: {circulating_supply}")
# # # #         print("-" * 50)
# # # #         print(f"Searching for {name} on CoinGecko...")
# # # #         print(json.dumps(search_data, indent=2))  # Pretty-print the full response for debugging


# # # # else:
# # # #     print(f"Failed to fetch data from DefiLlama. HTTP Status code: {response.status_code}")




# # # import requests
# # # import json
# # # import time

# # # # DefiLlama API for TVL data
# # # defi_url = "https://api.llama.fi/chains"

# # # # CoinMarketCap API (requires an API key)
# # # cmc_api_key = "9ea91b4a-4bf5-4b1c-8987-dd8734905c9c"
# # # cmc_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

# # # # Send a GET request to DefiLlama API
# # # response = requests.get(defi_url)

# # # # Check if the request was successful
# # # if response.status_code == 200:
# # #     data = response.json()
# # #     # Sort by TVL in descending order and get the top 50
# # #     top_50_tvl = sorted(data, key=lambda x: x.get('tvl', 0), reverse=True)[:50]

# # #     for i, blockchain in enumerate(top_50_tvl, 1):
# # #         name = blockchain.get('name', 'N/A')
# # #         tvl = blockchain.get('tvl', 0)

# # #         print(f"Fetching market data for {name} from CoinMarketCap...")

# # #         # Make a request to CoinMarketCap
# # #         headers = {"X-CMC_PRO_API_KEY": cmc_api_key}
# # #         params = {"symbol": name, "convert": "USD"}

# # #         cmc_response = requests.get(cmc_url, headers=headers, params=params)

# # #         if cmc_response.status_code == 200:
# # #             cmc_data = cmc_response.json()

# # #             # Extract market data if available
# # #             if "data" in cmc_data and name in cmc_data["data"]:
# # #                 coin_data = cmc_data["data"][name]["quote"]["USD"]
# # #                 price = coin_data.get("price", "N/A")
# # #                 market_cap = coin_data.get("market_cap", "N/A")
# # #                 volume_24h = coin_data.get("volume_24h", "N/A")
# # #                 circulating_supply = cmc_data["data"][name].get("circulating_supply", "N/A")
# # #             else:
# # #                 print(f"No market data found for {name} on CoinMarketCap.")
# # #                 price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'
# # #         else:
# # #             print(f"Failed to fetch market data for {name}. Status Code: {cmc_response.status_code}")
# # #             price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'

# # #         # Display results
# # #         print(f"{i}. {name} - TVL: ${tvl:,.2f}")
# # #         def format_currency(value):
# # #             return f"${value:,.2f}" if isinstance(value, (int, float)) else "N/A"

# # #         print(f"   Price: {format_currency(price)}")
# # #         print(f"   Market Cap: {format_currency(market_cap)}")
# # #         print(f"   24h Volume: {format_currency(volume_24h)}")
# # #         print(f"   Circulating Supply: {circulating_supply if isinstance(circulating_supply, (int, float)) else 'N/A'}")

        
# # #         print("-" * 50)

# # #         time.sleep(1)  # Prevent hitting rate limits

# # # else:
# # #     print(f"Failed to fetch TVL data from DefiLlama. Status code: {response.status_code}")


# # import requests
# # import time

# # # API Keys and Endpoints
# # defi_url = "https://api.llama.fi/chains"
# # cmc_api_key = "9ea91b4a-4bf5-4b1c-8987-dd8734905c9c"
# # cmc_map_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
# # cmc_quote_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

# # # Get CoinMarketCap coin mappings
# # headers = {"X-CMC_PRO_API_KEY": cmc_api_key}
# # map_response = requests.get(cmc_map_url, headers=headers)
# # if map_response.status_code == 200:
# #     cmc_map = {coin["name"]: coin["symbol"] for coin in map_response.json()["data"]}
# # else:
# #     print("Failed to fetch CoinMarketCap coin mapping.")
# #     cmc_map = {}

# # # Get top 50 TVL from DefiLlama
# # response = requests.get(defi_url)
# # if response.status_code == 200:
# #     data = response.json()
# #     top_50_tvl = sorted(data, key=lambda x: x.get('tvl', 0), reverse=True)[:50]

# #     for i, blockchain in enumerate(top_50_tvl, 1):
# #         name = blockchain.get('name', 'N/A')
# #         tvl = blockchain.get('tvl', 0)

# #         # Get the correct symbol from CoinMarketCap
# #         symbol = cmc_map.get(name)
# #         if not symbol:
# #             print(f"⚠️ No symbol found for {name}, skipping...")
# #             continue

# #         print(f"Fetching market data for {name} ({symbol}) from CoinMarketCap...")

# #         # Request CoinMarketCap data
# #         params = {"symbol": symbol, "convert": "USD"}
# #         cmc_response = requests.get(cmc_quote_url, headers=headers, params=params)

# #         if cmc_response.status_code == 200:
# #             cmc_data = cmc_response.json()
# #             if "data" in cmc_data and symbol in cmc_data["data"]:
# #                 coin_data = cmc_data["data"][symbol]["quote"]["USD"]
# #                 price = coin_data.get("price", "N/A")
# #                 market_cap = coin_data.get("market_cap", "N/A")
# #                 volume_24h = coin_data.get("volume_24h", "N/A")
# #                 circulating_supply = cmc_data["data"][symbol].get("circulating_supply", "N/A")
# #             else:
# #                 print(f"⚠️ No market data found for {name} ({symbol})")
# #                 price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'
# #         else:
# #             print(f"❌ Failed to fetch market data for {name} ({symbol}). Status Code: {cmc_response.status_code}")
# #             price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'

# #         # Print results
# #         def format_currency(value):
# #             return f"${value:,.2f}" if isinstance(value, (int, float)) else "N/A"

# #         print(f"{i}. {name} - TVL: ${tvl:,.2f}")
# #         print(f"   Price: {format_currency(price)}")
# #         print(f"   Market Cap: {format_currency(market_cap)}")
# #         print(f"   24h Volume: {format_currency(volume_24h)}")
# #         print(f"   Circulating Supply: {circulating_supply if isinstance(circulating_supply, (int, float)) else 'N/A'}")
# #         print("-" * 50)

# #         time.sleep(1)  # Prevent hitting API limits

# # else:
# #     print(f"❌ Failed to fetch TVL data from DefiLlama. Status code: {response.status_code}")


# # import requests
# # import time
# # import difflib

# # # API Keys and Endpoints
# # defi_url = "https://api.llama.fi/chains"
# # cmc_api_key = "9ea91b4a-4bf5-4b1c-8987-dd8734905c9c"
# # cmc_map_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
# # cmc_quote_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

# # # Known manual name-to-symbol mappings (for mismatched names)
# # manual_mappings = {
# #     "Ethereum": "ETH",
# #     "Binance": "BNB",
# #     "Polygon": "MATIC",
# #     "Arbitrum": "ARB",
# #     "Optimism": "OP",
# #     "Fantom": "FTM",
# #     "Avalanche": "AVAX",
# #     "Solana": "SOL",
# #     "Cronos": "CRO",
# #     "Celo": "CELO",
# #     "Moonbeam": "GLMR",
# #     "Moonriver": "MOVR",
# #     "RSK": "RBTC",
# #     "Harmony": "ONE",
# #     "Velas": "VLX",
# #     "Oasis": "ROSE",
# # }

# # # Fetch CoinMarketCap coin list
# # headers = {"X-CMC_PRO_API_KEY": cmc_api_key}
# # map_response = requests.get(cmc_map_url, headers=headers)

# # if map_response.status_code == 200:
# #     cmc_data = map_response.json()["data"]
    
# #     # Create a dictionary mapping names & slugs to symbols
# #     cmc_map = {coin["name"]: coin["symbol"] for coin in cmc_data}
# #     cmc_map.update({coin["slug"]: coin["symbol"] for coin in cmc_data})

# # else:
# #     print("❌ Failed to fetch CoinMarketCap coin mapping.")
# #     cmc_map = {}

# # # Get top 50 TVL from DefiLlama
# # response = requests.get(defi_url)
# # if response.status_code == 200:
# #     data = response.json()
# #     top_50_tvl = sorted(data, key=lambda x: x.get('tvl', 0), reverse=True)[:50]

# #     for i, blockchain in enumerate(top_50_tvl, 1):
# #         name = blockchain.get('name', 'N/A')
# #         tvl = blockchain.get('tvl', 0)

# #         # Check manual mappings first
# #         if name in manual_mappings:
# #             symbol = manual_mappings[name]
# #         else:
# #             # Try exact match
# #             symbol = cmc_map.get(name)
            
# #             # If no exact match, try fuzzy matching
# #             if not symbol:
# #                 possible_matches = difflib.get_close_matches(name, cmc_map.keys(), n=1, cutoff=0.7)
# #                 symbol = cmc_map.get(possible_matches[0]) if possible_matches else None
        
# #         if not symbol:
# #             print(f"⚠️ No symbol found for {name}, skipping...")
# #             continue

# #         print(f"Fetching market data for {name} ({symbol}) from CoinMarketCap...")

# #         # Request CoinMarketCap data
# #         params = {"symbol": symbol, "convert": "USD"}
# #         cmc_response = requests.get(cmc_quote_url, headers=headers, params=params)

# #         if cmc_response.status_code == 200:
# #             cmc_data = cmc_response.json()
# #             if "data" in cmc_data and symbol in cmc_data["data"]:
# #                 coin_data = cmc_data["data"][symbol]["quote"]["USD"]
# #                 price = coin_data.get("price", "N/A")
# #                 market_cap = coin_data.get("market_cap", "N/A")
# #                 volume_24h = coin_data.get("volume_24h", "N/A")
# #                 circulating_supply = cmc_data["data"][symbol].get("circulating_supply", "N/A")
# #             else:
# #                 print(f"⚠️ No market data found for {name} ({symbol})")
# #                 price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'
# #         else:
# #             print(f"❌ Failed to fetch market data for {name} ({symbol}). Status Code: {cmc_response.status_code}")
# #             price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'

# #         # Print results
# #         def format_currency(value):
# #             return f"${value:,.2f}" if isinstance(value, (int, float)) else "N/A"

# #         print(f"{i}. {name} - TVL: ${tvl:,.2f}")
# #         print(f"   Price: {format_currency(price)}")
# #         print(f"   Market Cap: {format_currency(market_cap)}")
# #         print(f"   24h Volume: {format_currency(volume_24h)}")
# #         print(f"   Circulating Supply: {circulating_supply if isinstance(circulating_supply, (int, float)) else 'N/A'}")
# #         print("-" * 50)

# #         time.sleep(1)  # Prevent hitting API limits

# # else:
# #     print(f"❌ Failed to fetch TVL data from DefiLlama. Status code: {response.status_code}")


import requests
import time
import difflib
import os
import sys
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()

# API Keys and Endpoints
defi_url = "https://api.llama.fi/chains"
cmc_api_key = os.getenv('cmc_api_key')
cmc_map_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
cmc_quote_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

# Known manual name-to-symbol mappings (for mismatched names)
manual_mappings = {
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

# Fetch CoinMarketCap coin list
headers = {"X-CMC_PRO_API_KEY": cmc_api_key}
map_response = requests.get(cmc_map_url, headers=headers)

if map_response.status_code == 200:
    cmc_data = map_response.json()["data"]
    
    # Create a dictionary mapping names & slugs to symbols
    cmc_map = {coin["name"]: coin["symbol"] for coin in cmc_data}
    cmc_map.update({coin["slug"]: coin["symbol"] for coin in cmc_data})

else:
    print("❌ Failed to fetch CoinMarketCap coin mapping.")
    cmc_map = {}

# Define a set of excluded names
excluded_names = {"Bahamut"}

# Get top 50 TVL from DefiLlama
response = requests.get(defi_url)
if response.status_code == 200:
    data = response.json()

    # Sort by TVL and filter out excluded names
    top_50_tvl = sorted(
        [chain for chain in data if chain.get('name') not in excluded_names], 
        key=lambda x: x.get('tvl', 0), 
        reverse=True
    )[:50]  # Ensure we still get 50 valid entries

    # Collect symbols for batch requests
    coins = []
    missing_coins = []

    for blockchain in top_50_tvl:
        name = blockchain.get('name', 'N/A')
        tvl = blockchain.get('tvl', 0)

        # Check manual mappings first
        if name in manual_mappings:
            symbol = manual_mappings[name]
        else:
            # Try exact match
            symbol = cmc_map.get(name)
            
            # If no exact match, try fuzzy matching
            if not symbol:
                possible_matches = difflib.get_close_matches(name, cmc_map.keys(), n=1, cutoff=0.7)
                symbol = cmc_map.get(possible_matches[0]) if possible_matches else None
        
        if not symbol:
            print(f"⚠️ No symbol found for {name}, skipping market data fetch...")
            missing_coins.append(name)
            continue

        coins.append({"name": name, "symbol": symbol, "tvl": tvl})

    # Batch process market data requests (max 10 symbols per request)
    batch_size = 10
    for i in range(0, len(coins), batch_size):
        batch = coins[i:i + batch_size]
        symbols_query = ",".join([coin["symbol"] for coin in batch])

        print(f"Fetching market data for batch: {symbols_query}")

        # Request CoinMarketCap data
        params = {"symbol": symbols_query, "convert": "USD"}
        cmc_response = requests.get(cmc_quote_url, headers=headers, params=params)

        if cmc_response.status_code == 200:
            cmc_data = cmc_response.json().get("data", {})

            for coin in batch:
                symbol = coin["symbol"]
                name = coin["name"]
                tvl = coin["tvl"]

                if symbol in cmc_data:
                    coin_data = cmc_data[symbol]["quote"]["USD"]
                    price = coin_data.get("price", "N/A")
                    market_cap = coin_data.get("market_cap", "N/A")
                    volume_24h = coin_data.get("volume_24h", "N/A")
                    circulating_supply = cmc_data[symbol].get("circulating_supply", "N/A")
                else:
                    print(f"⚠️ No market data found for {name} ({symbol})")
                    price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'

                # Print results
                def format_currency(value):
                    return f"${value:,.2f}" if isinstance(value, (int, float)) else "N/A"

                print(f"{name} ({symbol}) - TVL: {format_currency(tvl)}")
                print(f"   Price: {format_currency(price)}")
                print(f"   Market Cap: {format_currency(market_cap)}")
                print(f"   24h Volume: {format_currency(volume_24h)}")
                print(f"   Circulating Supply: {circulating_supply if isinstance(circulating_supply, (int, float)) else 'N/A'}")
                print("-" * 50)

        else:
            print(f"❌ Failed to fetch market data for batch. Status Code: {cmc_response.status_code}")

        # Sleep to avoid hitting API limits
        time.sleep(30)  # Increase delay if 429 errors persist

    # Log missing coins to a file
    if missing_coins:
        with open("missing_coins.txt", "w") as f:
            for coin in missing_coins:
                f.write(f"{coin}\n")
        print("⚠️ Missing coins saved to 'missing_coins.txt' for manual review.")

else:
    print(f"❌ Failed to fetch TVL data from DefiLlama. Status code: {response.status_code}")



import pandas as pd  # Add this import for Pandas

# Collect data for the DataFrame
data_for_dataframe = []


# Batch process market data requests (max 10 symbols per request)
batch_size = 10
for i in range(0, len(coins), batch_size):
    batch = coins[i:i + batch_size]
    symbols_query = ",".join([coin["symbol"] for coin in batch])

    print(f"Fetching market data for batch: {symbols_query}")

    # Request CoinMarketCap data
    params = {"symbol": symbols_query, "convert": "USD"}
    cmc_response = requests.get(cmc_quote_url, headers=headers, params=params)

    if cmc_response.status_code == 200:
        cmc_data = cmc_response.json().get("data", {})

        for coin in batch:
            symbol = coin["symbol"]
            name = coin["name"]
            tvl = coin["tvl"]

            if symbol in cmc_data:
                coin_data = cmc_data[symbol]["quote"]["USD"]
                price = coin_data.get("price", "N/A")
                market_cap = coin_data.get("market_cap", "N/A")
                volume_24h = coin_data.get("volume_24h", "N/A")
                circulating_supply = cmc_data[symbol].get("circulating_supply", "N/A")
            else:
                print(f"⚠️ No market data found for {name} ({symbol})")
                price, market_cap, volume_24h, circulating_supply = 'N/A', 'N/A', 'N/A', 'N/A'

            # Append data to the list for the DataFrame
            data_for_dataframe.append({
                "Name": name,
                "Symbol": symbol,
                "TVL": tvl,
                "Price (USD)": price,
                "Market Cap (USD)": market_cap,
                "24h Volume (USD)": volume_24h,
                "Circulating Supply": circulating_supply
            })

            # Print results
            def format_currency(value):
                return f"${value:,.2f}" if isinstance(value, (int, float)) else "N/A"

            print(f"{name} ({symbol}) - TVL: {format_currency(tvl)}")
            print(f"   Price: {format_currency(price)}")
            print(f"   Market Cap: {format_currency(market_cap)}")
            print(f"   24h Volume: {format_currency(volume_24h)}")
            print(f"   Circulating Supply: {circulating_supply if isinstance(circulating_supply, (int, float)) else 'N/A'}")
            print("-" * 50)

    else:
        print(f"❌ Failed to fetch market data for batch. Status Code: {cmc_response.status_code}")

    # Sleep to avoid hitting API limits
    time.sleep(30)  # Increase delay if 429 errors persist

# Create a DataFrame from the collected data
df = pd.DataFrame(data_for_dataframe)

# Save the DataFrame to a CSV file (optional)
df.to_csv("output_data.csv", index=False)

# Display the DataFrame
print(df)