FROM python:3.9-slim

# Version 2.1 - Fixed entrypoint order with specific copy operation

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

# Create a directory for logs
RUN mkdir -p /app/logs

# Copy project files EXCEPT entrypoint.sh (which will be copied separately)
COPY . .

# Debug - list files in app directory
RUN ls -la /app/

# Copy entrypoint script separately and make executable (after copying project files)
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Debug - verify entrypoint exists
RUN ls -la /app/entrypoint.sh

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Default command (show help)
CMD ["help"] 