import os
import sys
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def test_environment_variables():
    """Test if all required environment variables are set."""
    # Database variables
    db_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT"]
    db_status = all(os.getenv(var) for var in db_vars)
    
    # API Keys
    api_keys = {
        "CoinMarketCap": os.getenv("cmc_api_key"),
        "OpenAI": os.getenv("OPENAI_API_KEY")
    }
    api_status = all(api_keys.values())
    
    # Reddit credentials
    reddit_vars = ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET"]
    reddit_status = all(os.getenv(var) for var in reddit_vars)
    
    # Print status
    logger.info("Environment Variables Check:")
    logger.info(f"  Database Variables: {'All Present' if db_status else 'Missing Some'}")
    for var in db_vars:
        logger.info(f"    {var}: {'✓' if os.getenv(var) else '✗'}")
    
    logger.info(f"  API Keys: {'All Present' if api_status else 'Missing Some'}")
    for key, value in api_keys.items():
        logger.info(f"    {key}: {'✓' if value else '✗'}")
    
    logger.info(f"  Reddit Credentials: {'All Present' if reddit_status else 'Missing Some'}")
    for var in reddit_vars:
        logger.info(f"    {var}: {'✓' if os.getenv(var) else '✗'}")
    
    return db_status and api_status and reddit_status

def simulate_workflow():
    """Simulate the workflow to verify it would work in GitHub Actions."""
    logger.info("Simulating full workflow:")
    
    # Step 1: DeFi Llama Data Scraping
    logger.info("Step 1: DeFi Llama Data Scraping")
    try:
        from DefiLlama_mock import generate_mock_data
        mock_data = generate_mock_data()
        logger.info(f"  Generated {len(mock_data)} mock entries")
        logger.info("  Status: ✓")
    except Exception as e:
        logger.error(f"  Error: {e}")
        logger.info("  Status: ✗")
    
    # Step 2: Reddit Data Scraping
    logger.info("Step 2: Reddit Data Scraping")
    try:
        # Import without running the actual script
        import Reddit_scraper
        logger.info("  Reddit scraper imported successfully")
        logger.info("  Status: ✓")
    except Exception as e:
        logger.error(f"  Error: {e}")
        logger.info("  Status: ✗")
    
    # Step 3: RAG System
    logger.info("Step 3: RAG System Testing")
    try:
        # Import without running the actual script
        import RAG
        logger.info("  RAG system imported successfully")
        logger.info("  Status: ✓")
    except Exception as e:
        logger.error(f"  Error: {e}")
        logger.info("  Status: ✗")
    
    logger.info("Workflow simulation completed")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test GitHub Workflows')
    parser.add_argument('--mock', action='store_true', help='Run in mock mode')
    args = parser.parse_args()
    
    if args.mock:
        logger.info("Running in MOCK mode - using mock data and services")
        # Set environment variables for testing in mock mode
        os.environ["DB_HOST"] = "mock_host"
        os.environ["DB_NAME"] = "mock_db"
        os.environ["DB_USER"] = "mock_user"
        os.environ["DB_PASSWORD"] = "mock_password"
        os.environ["DB_PORT"] = "5432"
        os.environ["cmc_api_key"] = "mock_cmc_key"
        os.environ["OPENAI_API_KEY"] = "mock_openai_key"
        os.environ["REDDIT_CLIENT_ID"] = "mock_reddit_id"
        os.environ["REDDIT_CLIENT_SECRET"] = "mock_reddit_secret"
    
    # Check environment variables
    env_status = test_environment_variables()
    
    # Simulate workflow
    simulate_workflow()
    
    if env_status:
        logger.info("All required environment variables are set. GitHub Actions should work.")
    else:
        if args.mock:
            logger.info("Running in mock mode, so missing real credentials is expected.")
        else:
            logger.warning("Some environment variables are missing. GitHub Actions may fail without them.")

if __name__ == "__main__":
    main() 