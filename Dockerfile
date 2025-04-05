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
elif [ "$1" = "langchain-rag" ]; then\n\
    python langchain_rag.py "${@:2}"\n\
elif [ "$1" = "example" ]; then\n\
    python example_usage.py "${@:2}"\n\
elif [ "$1" = "migrate" ]; then\n\
    python migrate_to_langchain.py "${@:2}"\n\
elif [ "$1" = "compare" ]; then\n\
    python compare_rag_implementations.py "${@:2}"\n\
elif [ "$1" = "test" ]; then\n\
    python test_pgvector.py\n\
elif [ "$1" = "check" ]; then\n\
    python check.py\n\
elif [ "$1" = "lineage" ]; then\n\
    echo "Generating data lineage for all pipelines..."\n\
    python DefiLlama_scraper.py --mock\n\
    python Reddit_scraper.py --mock\n\
    python improved_RAG.py --mock --query "Bitcoin"\n\
    echo "Lineage visualizations generated in /app/visualizations/"\n\
else\n\
    echo "Usage: docker run [options] <image> [command] [args]"\n\
    echo ""\n\
    echo "Commands:"\n\
    echo "  defi           Run DeFi Llama scraper"\n\
    echo "  reddit         Run Reddit scraper"\n\
    echo "  rag            Run improved RAG system"\n\
    echo "  langchain-rag  Run LangChain RAG implementation"\n\
    echo "  example        Run example usage script with interactive or batch mode"\n\
    echo "  migrate        Run migration script to convert embeddings to LangChain format"\n\
    echo "  compare        Compare different RAG implementations"\n\
    echo "  test           Run pgvector tests"\n\
    echo "  check          Run database checks"\n\
    echo "  lineage        Generate data lineage visualizations"\n\
fi' > /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Default command (show help)
CMD ["help"] 