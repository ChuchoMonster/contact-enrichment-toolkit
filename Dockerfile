FROM python:3.11-slim

WORKDIR /app

# Install Node.js for gws CLI
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install gws CLI globally
RUN npm install -g @googleworkspace/cli

COPY requirements-web.txt .
RUN pip install --no-cache-dir -r requirements-web.txt

COPY blitz_core.py .
COPY web/ web/

# SQLite DB on persistent volume
ENV DB_PATH=/data/blitz_web.db

EXPOSE 8080

CMD ["uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8080"]
