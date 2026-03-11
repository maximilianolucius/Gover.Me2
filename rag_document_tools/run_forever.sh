#!/bin/bash
while true; do
    python rag_document_tools/run_scraper_parallel.py
    echo "Script completed. Restarting in 10 seconds..."
    sleep 1800
done
