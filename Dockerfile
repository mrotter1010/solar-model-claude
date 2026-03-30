FROM python:3.12-slim

# System deps for psycopg2-binary, rasterio, GDAL, and general compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    libgdal-dev \
    gdal-bin \
    libgeos-dev \
    libproj-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer caching)
COPY requirements-docker.txt .
RUN pip install --no-cache-dir -r requirements-docker.txt

# Copy alembic config and migrations
COPY alembic.ini .
COPY alembic/ alembic/

# Copy application source
COPY src/ src/

# Copy CEC database files (modules + inverters)
COPY docs/CEC\ Modules.csv docs/CEC\ Modules.csv
COPY docs/CEC\ Inverters.csv docs/CEC\ Inverters.csv

# Copy data directory (load profiles, LMP cache, etc.)
# Note: data/ is in .gitignore — build context must include it.
COPY data/ data/

# Copy entrypoint script
COPY scripts/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Create directories for runtime volumes
RUN mkdir -p /app/uploads /app/outputs

EXPOSE 8000

ENTRYPOINT ["/docker-entrypoint.sh"]
