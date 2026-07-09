# Single image containing all services + shared code. Each service is launched
# from the same image via a different uvicorn command in docker-compose. This
# keeps the code DRY while still giving every service its own process/container.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default command; overridden per-service in docker-compose.
CMD ["uvicorn", "gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
