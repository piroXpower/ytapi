FROM python:3.10-slim-bullseye

# Install system dependencies, C++ compiler, and FFmpeg for PyTgCalls
RUN apt-get update -y && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        git \
        build-essential \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency definition and install wheels
COPY requirements.txt .
RUN pip install --no-cache-dir -U pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# Copy server code
COPY . .

# Default Koyeb port
ENV PORT=8080
EXPOSE 8080

# Start assistant server
CMD ["python3", "assistant_server.py"]
