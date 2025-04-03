# Crypto Data Engineering

This repository contains tools for collecting, processing, and analyzing cryptocurrency data from various sources.

## Features

- **DeFi Llama Integration**: Fetches Total Value Locked (TVL) metrics for blockchain networks
- **CoinMarketCap Integration**: Retrieves market data including prices, volumes, and market caps
- **Reddit Data Scraping**: Collects posts from cryptocurrency subreddits with OpenAI embeddings
- **PostgreSQL Storage**: Structures and stores data in a PostgreSQL database
- **Automated Workflows**: Uses GitHub Actions for scheduled data collection

## Components

### 1. DeFi Llama Data Pipeline

The DeFi Llama pipeline collects TVL data and market metrics:

- **DefiLlama_scraper.py**: Fetches data from DeFi Llama and CoinMarketCap APIs
- **DefiLlama_to_postgresql.py**: Uploads processed data to PostgreSQL
- **check.py**: Verifies data quality and database connectivity

### 2. Reddit Data Pipeline

The Reddit pipeline collects and analyzes cryptocurrency discussions:

- **Reddit_scraper.py**: Fetches posts from r/cryptocurrency and generates embeddings
- Supports both normal and mock mode for testing

## Setup and Installation

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file with the following environment variables:
   ```
   # API Keys
   cmc_api_key=your_coinmarketcap_api_key
   REDDIT_CLIENT_ID=your_reddit_client_id
   REDDIT_CLIENT_SECRET=your_reddit_client_secret
   REDDIT_USER_AGENT=your_reddit_user_agent
   OPENAI_API_KEY=your_openai_api_key

   # Database Configuration
   DB_HOST=your_database_host
   DB_PORT=your_database_port
   DB_NAME=your_database_name
   DB_USER=your_database_user
   DB_PASSWORD=your_database_password
   ```

## Usage

### DeFi Llama Data Collection

```bash
# Run the DeFi Llama scraper
python DefiLlama_scraper.py

# Upload data to PostgreSQL
python DefiLlama_to_postgresql.py

# Verify the data
python check.py
```

### Reddit Data Collection

```bash
# Run the Reddit scraper
python Reddit_scraper.py

# Run in mock mode (for testing)
python Reddit_scraper.py --mock
```

## Automated Workflows

This repository includes GitHub Actions workflows that run on a schedule. To enable them:

1. Add all required secrets to your GitHub repository
2. The workflows will run automatically every 6 hours
3. You can also trigger them manually from the Actions tab

## Recent Improvements

- **Modular Code Design**: Refactored code into small, testable functions
- **Enhanced Error Handling**: Added robust error handling with informative messages
- **Proper Logging**: Implemented structured logging throughout the codebase
- **Data Validation**: Added data validation to ensure quality
- **Connection Management**: Improved database connection handling
- **Upsert Strategy**: Changed from replace to upsert to preserve historical data
- **Documentation**: Added comprehensive code comments and user documentation

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 