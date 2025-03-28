# Crypto Data Engineering Project

## Overview  
This project is designed to collect, process, and store structured and unstructured cryptocurrency data. The goal is to build a scalable data pipeline that aggregates information from multiple sources, including, DeFiLlama for structured data and reddit or twitter for unstructured data. Future enhancements will include sentiment analysis from the unstructured data, as well as a chatbot for querying insights.

## Features  
- Scrapes structured cryptocurrency data, including price, volume, and market metrics.  
- Stores data in a PostgreSQL database for efficient querying and scalability.  
- Automates periodic data collection to ensure real-time updates.  
- Future development includes sentiment analysis and an LLM-powered chatbot for information retrieval.  

## Tech Stack  
- **Python** – Data scraping and processing  
- **PostgreSQL** – Database for structured data storage
- **MongoDB** - Database for unstructured data storage
- **Jira & GitHub** – Project management and version control

## Getting Started  

### 1. Clone the Repository  
```bash
git clone https://github.com/tchris140/crypto-data-engineering.git
cd crypto-data-engineering
