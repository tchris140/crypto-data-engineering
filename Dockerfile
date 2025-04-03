FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install PostgreSQL client and build dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install pgvector from source
RUN git clone https://github.com/pgvector/pgvector.git \
    && cd pgvector \
    && make \
    && cd ..

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create a directory for logs
RUN mkdir -p /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create an entrypoint script
RUN echo '#!/bin/bash\n\
if [ "$1" = "defi" ]; then\n\
    python DefiLlama_scraper.py "${@:2}"\n\
elif [ "$1" = "reddit" ]; then\n\
    python Reddit_scraper.py "${@:2}"\n\
elif [ "$1" = "rag" ]; then\n\
    python improved_RAG.py "${@:2}"\n\
elif [ "$1" = "test" ]; then\n\
    python test_pgvector.py\n\
elif [ "$1" = "check" ]; then\n\
    python check.py\n\
else\n\
    echo "Usage: docker run [options] <image> [command] [args]"\n\
    echo ""\n\
    echo "Commands:"\n\
    echo "  defi    Run DeFi Llama scraper"\n\
    echo "  reddit  Run Reddit scraper"\n\
    echo "  rag     Run improved RAG system"\n\
    echo "  test    Run pgvector tests"\n\
    echo "  check   Run database checks"\n\
fi' > /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Default command (show help)
CMD ["help"] 