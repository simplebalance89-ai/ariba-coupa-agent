# Ariba/Coupa PO Automation Agent
# FastAPI + Azure Blob + P21 CISM Integration

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create directories — /app/data is the Render persistent disk mount point
RUN mkdir -p /app/data/cism_output /app/data/cism_so_output /app/data/crosswalks \
    /app/data/po_store /app/data/p21_data /app/data/quote_data /app/logs /app/test_data

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Run the application
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]

