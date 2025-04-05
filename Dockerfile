FROM python:3.9-slim

# Version 2.0 - pgvector build removed and using pre-built image

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

# Create visualization directory
RUN mkdir -p /app/visualizations

# Copy entrypoint script and make executable
COPY entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

# Copy project files
COPY . .

# Create a directory for logs
RUN mkdir -p /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Default command (show help)
CMD ["help"] 