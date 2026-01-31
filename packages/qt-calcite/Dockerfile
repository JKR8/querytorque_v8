# QTCalcite - LLM-R2 SQL Query Optimizer
# Multi-stage build: Java + Python

FROM eclipse-temurin:17-jdk AS builder

WORKDIR /build
COPY build.gradle.kts settings.gradle.kts gradlew ./
COPY gradle ./gradle
COPY src ./src

RUN chmod +x gradlew && ./gradlew fatJar --no-daemon

# Runtime image
FROM eclipse-temurin:17-jre

# Install Python and curl (for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy JAR from builder
COPY --from=builder /build/build/libs/qt-calcite-*-all.jar /app/qt-calcite.jar

# Copy Python API
COPY api/ /app/api/
COPY requirements.txt /app/

# Install Python deps
RUN pip3 install --no-cache-dir -r requirements.txt

# Environment
ENV QTCALCITE_DB=/data/database.duckdb
ENV DEEPSEEK_API_KEY=""

EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["python3", "-m", "api.main", "--host", "0.0.0.0", "--port", "8000"]
