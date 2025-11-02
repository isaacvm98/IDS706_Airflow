FROM apache/airflow:2.8.1-python3.11

# Switch to root to install system dependencies
USER root

# Install system dependencies including Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --user -r /requirements.txt

# Set working directory
WORKDIR /opt/airflow