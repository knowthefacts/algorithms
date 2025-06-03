# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user for security
RUN useradd -m -u 1000 streamlit && chown -R streamlit:streamlit /app
USER streamlit

# Expose port
EXPOSE 8501

# Note: Health check is handled by ALB Target Group
# No Docker HEALTHCHECK needed as ALB will check /_stcore/health endpoint

# Command to run the application
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.baseUrlPath=/app"]
