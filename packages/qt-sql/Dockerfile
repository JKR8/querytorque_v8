# qt-sql - SQL Optimization Product
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY pyproject.toml ./
COPY qt_sql/ ./qt_sql/
COPY api/ ./api/

# Install qt-shared from local path (in production, use pip install qt-shared)
# For now, copy it
COPY ../qt-shared/qt_shared /app/qt_shared
COPY ../qt-shared/pyproject.toml /app/qt_shared_pyproject.toml

# Install dependencies
RUN pip install --no-cache-dir -e . || pip install --no-cache-dir .

# Environment
ENV PYTHONUNBUFFERED=1
ENV QT_DATABASE_URL=""
ENV QTCALCITE_URL="http://qt-calcite:8000"

EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
