FROM python:3.11-slim

# Install FFmpeg for video conversion
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create temp directories
RUN mkdir -p /tmp/securecam_uploads /tmp/securecam_mp4

# Expose port
EXPOSE 8080

# Run with gunicorn for production
CMD gunicorn --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 300 server:app
