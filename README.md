# Crypto Data Engineering

This repository contains scripts for scraping cryptocurrency data from various sources, processing the data, and storing it in a PostgreSQL database.

## Reddit Data Scraper

The `Reddit_scraper.py` script fetches recent posts from the r/cryptocurrency subreddit, generates embeddings using OpenAI's API, and stores the data in a PostgreSQL database.

### Features

- Fetches recent cryptocurrency posts from Reddit
- Generates embeddings for each post using OpenAI's API
- Stores the data in a PostgreSQL database
- Handles error cases gracefully
- Includes a mock mode for testing without making actual API calls

### Setup

1. Clone this repository
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file with the following environment variables:
   ```
   # Reddit API credentials
   REDDIT_CLIENT_ID=your_client_id_here
   REDDIT_CLIENT_SECRET=your_client_secret_here
   REDDIT_USER_AGENT=your_user_agent_here

   # OpenAI API key
   OPENAI_API_KEY=your_openai_key_here

   # Database credentials
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   DB_HOST=your_db_host
   DB_PORT=your_db_port
   DB_NAME=your_db_name
   ```

### Usage

Run the script normally to fetch real data:
```bash
python Reddit_scraper.py
```

Run in mock mode for testing (no actual API calls):
```bash
python Reddit_scraper.py --mock
```

Run the test suite:
```bash
python test_reddit_scraper.py
```

### GitHub Actions

This repository includes a GitHub Actions workflow that runs the script every 6 hours. To use this:

1. Set up the following secrets in your GitHub repository:
   - `REDDIT_CLIENT_ID`
   - `REDDIT_CLIENT_SECRET`
   - `REDDIT_USER_AGENT`
   - `OPENAI_API_KEY`
   - `DB_USER`
   - `DB_PASSWORD`
   - `DB_HOST`
   - `DB_PORT`
   - `DB_NAME`

2. The workflow will automatically run on schedule, or you can trigger it manually from the Actions tab.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 