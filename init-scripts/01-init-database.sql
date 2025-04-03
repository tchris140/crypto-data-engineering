-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Create tables
CREATE TABLE IF NOT EXISTS reddit_embeddings (
    post_id TEXT PRIMARY KEY,
    title TEXT,
    text TEXT,
    score INTEGER,
    num_comments INTEGER,
    created_utc TEXT,
    embedding TEXT,
    embedding_vector vector(1536)
);

CREATE TABLE IF NOT EXISTS coin_data_structured (
    "Name" TEXT,
    "Symbol" TEXT,
    "Price (USD)" NUMERIC,
    "Market Cap (USD)" NUMERIC,
    "24h Volume (USD)" NUMERIC,
    "Circulating Supply" NUMERIC,
    "Updated" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("Symbol")
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_reddit_title ON reddit_embeddings(title);
CREATE INDEX IF NOT EXISTS idx_coin_name ON coin_data_structured("Name");

-- Create vector indexes for similarity searches
CREATE INDEX IF NOT EXISTS idx_reddit_vector ON reddit_embeddings USING ivfflat (embedding_vector vector_cosine_ops) WITH (lists = 100);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO CURRENT_USER; 