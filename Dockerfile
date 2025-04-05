FROM python:3.9-slim

# Version 3.0 - Simplified build process with direct entrypoint

# Set working directory
WORKDIR /app

# Install PostgreSQL client and other necessary dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install data lineage visualization dependencies
RUN pip install --no-cache-dir networkx pyvis

# Create directories
RUN mkdir -p /app/visualizations /app/logs

# Copy entrypoint script first
COPY entrypoint.sh .
RUN chmod +x /app/entrypoint.sh

# Copy all project files
COPY . .

# Debug commands to verify file structure
RUN echo "Listing all files in /app:" && ls -la /app/
RUN echo "Checking entrypoint script:" && cat /app/entrypoint.sh

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Use shell form of ENTRYPOINT for better error handling
ENTRYPOINT /bin/bash /app/entrypoint.sh

# Default command (show help)
CMD ["help"] 