FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    LIBRARY_DIR=/calibre-library \
    STATE_DIR=/config

RUN pip install --no-cache-dir pillow lxml

WORKDIR /app
COPY crosspoint/ ./crosspoint/
COPY x3sync.py .

# Run as Unraid's nobody:users by default; override with --user if needed.
USER 99:100

CMD ["python", "/app/x3sync.py"]
