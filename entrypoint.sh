#!/bin/bash

if [ "$1" = "defi" ]; then
    python DefiLlama_scraper.py "${@:2}"
elif [ "$1" = "reddit" ]; then
    python Reddit_scraper.py "${@:2}"
elif [ "$1" = "rag" ]; then
    python improved_RAG.py "${@:2}"
elif [ "$1" = "langchain-rag" ]; then
    python langchain_rag.py "${@:2}"
elif [ "$1" = "example" ]; then
    python example_usage.py "${@:2}"
elif [ "$1" = "migrate" ]; then
    python migrate_to_langchain.py "${@:2}"
elif [ "$1" = "compare" ]; then
    python compare_rag_implementations.py "${@:2}"
elif [ "$1" = "test" ]; then
    python test_pgvector.py
elif [ "$1" = "check" ]; then
    python check.py
elif [ "$1" = "lineage" ]; then
    echo "Generating data lineage for all pipelines..."
    python DefiLlama_scraper.py --mock
    python Reddit_scraper.py --mock
    python improved_RAG.py --mock --query "Bitcoin"
    echo "Lineage visualizations generated in /app/visualizations/"
else
    echo "Usage: docker run [options] <image> [command] [args]"
    echo ""
    echo "Commands:"
    echo "  defi           Run DeFi Llama scraper"
    echo "  reddit         Run Reddit scraper"
    echo "  rag            Run improved RAG system"
    echo "  langchain-rag  Run LangChain RAG implementation"
    echo "  example        Run example usage script with interactive or batch mode"
    echo "  migrate        Run migration script to convert embeddings to LangChain format"
    echo "  compare        Compare different RAG implementations"
    echo "  test           Run pgvector tests"
    echo "  check          Run database checks"
    echo "  lineage        Generate data lineage visualizations"
fi
