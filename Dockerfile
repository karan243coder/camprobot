FROM python:3.11-slim

LABEL maintainer="MeetLink"
LABEL description="MeetLink Backend - Telegram Logger + File Sharing + Recording Converter"

WORKDIR /app

# Install ffmpeg for video/audio conversion (WebM → MP4 + MP3)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy server code and modules
COPY . .

# Create directories
RUN mkdir -p /tmp/meetlink_recordings /tmp/meetlink_uploads

# Expose port 8080
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/api/status')" || exit 1

# Run
CMD ["python", "server.py"]
