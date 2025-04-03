#!/bin/bash

# ANSI color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored messages
print_message() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    print_warning "Docker Compose not found. Attempting to use 'docker compose' command..."
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    print_message "Creating .env file..."
    cat > .env << EOL
# API Keys (replace with your actual keys)
cmc_api_key=your_coinmarketcap_api_key
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
REDDIT_USER_AGENT=your_reddit_user_agent
OPENAI_API_KEY=your_openai_api_key

# Database Configuration
DB_HOST=postgres
DB_PORT=5432
DB_NAME=crypto_data
DB_USER=postgres
DB_PASSWORD=postgres
EOL
    print_warning "Created default .env file. Please update it with your actual API keys."
fi

# Create logs directory if it doesn't exist
mkdir -p logs
print_message "Created logs directory."

# Function to display help
show_help() {
    echo "Crypto Data Engineering Setup Script"
    echo ""
    echo "Usage: ./setup.sh [command]"
    echo ""
    echo "Commands:"
    echo "  start          Build and start all containers"
    echo "  stop           Stop all containers"
    echo "  restart        Restart all containers"
    echo "  logs           Show logs from all containers"
    echo "  defi           Run DeFi Llama scraper"
    echo "  reddit         Run Reddit scraper"
    echo "  rag [query]    Run improved RAG system with optional query"
    echo "  test           Run pgvector tests"
    echo "  check          Run database checks"
    echo "  mock-all       Run all components in mock mode"
    echo "  lineage        Generate data lineage visualizations"
    echo "  clean          Remove all containers and volumes"
    echo "  help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./setup.sh start"
    echo "  ./setup.sh defi --mock"
    echo "  ./setup.sh rag \"Bitcoin price prediction\""
}

# Process commands
case "$1" in
    start)
        print_message "Building and starting containers..."
        $DOCKER_COMPOSE up -d
        print_message "Containers started! Use './setup.sh logs' to view logs."
        ;;
    stop)
        print_message "Stopping containers..."
        $DOCKER_COMPOSE down
        ;;
    restart)
        print_message "Restarting containers..."
        $DOCKER_COMPOSE restart
        ;;
    logs)
        print_message "Showing logs..."
        $DOCKER_COMPOSE logs -f
        ;;
    defi)
        print_message "Running DeFi Llama scraper..."
        shift
        $DOCKER_COMPOSE run --rm app defi "$@"
        ;;
    reddit)
        print_message "Running Reddit scraper..."
        shift
        $DOCKER_COMPOSE run --rm app reddit "$@"
        ;;
    rag)
        query="${2:-Ethereum}"
        shift
        print_message "Running RAG system with query: '$query'..."
        $DOCKER_COMPOSE run --rm app rag --query "$query" "${@:2}"
        ;;
    test)
        print_message "Running pgvector tests..."
        $DOCKER_COMPOSE run --rm app test
        ;;
    check)
        print_message "Running database checks..."
        $DOCKER_COMPOSE run --rm app check
        ;;
    mock-all)
        print_message "Running all components in mock mode..."
        print_message "1. Running DeFi Llama scraper in mock mode..."
        $DOCKER_COMPOSE run --rm app defi --mock
        print_message "2. Running Reddit scraper in mock mode..."
        $DOCKER_COMPOSE run --rm app reddit --mock
        print_message "3. Running RAG system in mock mode..."
        $DOCKER_COMPOSE run --rm app rag --mock --query "Bitcoin"
        ;;
    lineage)
        print_message "Generating data lineage visualizations..."
        $DOCKER_COMPOSE run --rm app lineage
        print_message "Data lineage visualizations generated. They are available in the visualizations/ directory."
        ;;
    clean)
        print_message "Removing all containers and volumes..."
        $DOCKER_COMPOSE down -v
        ;;
    help|*)
        show_help
        ;;
esac 