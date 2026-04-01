# Optional Fly.io deploy (always-on machine; avoids Render free cold starts).
FROM python:3.11-slim-bookworm

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8080
# Fly sets PORT; default 8080 for local docker run.
CMD ["sh", "-c", "exec gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 1 --threads 2 --timeout 120 app:server"]
