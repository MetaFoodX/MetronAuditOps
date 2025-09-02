# Multi-stage build for ML dependencies
FROM python:3.11-slim as base

# Install system dependencies for ML libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libsm6 libxext6 libxrender1 git curl build-essential \
    gettext-base libgomp1 libgl1-mesa-glx libglib2.0-0 \
    libavcodec-dev libavformat-dev libswscale-dev libv4l-dev \
    libxvidcore-dev libx264-dev libjpeg-dev libpng-dev libtiff-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the entire project structure first to include all AI/ML dependencies
COPY . /app/

# Install Python dependencies in layers for better caching
RUN pip install --no-cache-dir torch==2.1.2 torchvision==0.16.2 --index-url https://download.pytorch.org/whl/cpu

# Install other ML dependencies
RUN pip install --no-cache-dir opencv-python==4.9.0.80 ultralytics>=8.2.0

# Install remaining dependencies
RUN pip install --no-cache-dir -r backend/requirements.txt

# Create necessary directories
RUN mkdir -p /tmp/pan_per_scans /tmp/_auditsCompleted /tmp/_audits

# Set environment variables
ENV PORT=8080
ENV PYTHONPATH=/app:/app/backend:/app/audit_automation:/app/system:$PYTHONPATH
ENV PYTHONUNBUFFERED=1

# Make entrypoint executable
RUN chmod +x /app/backend/entrypoint.sh

# Use entrypoint script
ENTRYPOINT ["/app/backend/entrypoint.sh"]
