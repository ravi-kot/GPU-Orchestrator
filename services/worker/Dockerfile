FROM python:3.11-slim
WORKDIR /app
# OpenCV sometimes needs these libs; headless reduces dependencies but keep minimal runtime libs.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libsm6 libxrender1 libxext6 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY worker.py .
CMD ["python", "worker.py"]
