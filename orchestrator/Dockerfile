FROM python:3.12-slim

WORKDIR /app

# Install Python dependencies first (layer caching)
COPY orchestrator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy orchestrator package (imports use "orchestrator." prefix)
COPY orchestrator/ orchestrator/

EXPOSE 8001

CMD ["uvicorn", "orchestrator.app:app", "--host", "0.0.0.0", "--port", "8001"]
