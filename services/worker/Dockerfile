FROM python:3.11-slim
WORKDIR /app
COPY services/worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/worker /app
ENV REDIS_URL=redis://redis:6379/0
ENV JOB_MANAGER=http://job_manager:8000
CMD ["python", "worker.py"]
