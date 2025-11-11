FROM python:3.13-slim

# Install PostgreSQL client libs for psycopg2-binary
RUN pip install --no-cache-dir psycopg2-binary

WORKDIR /app
COPY init_postgis.py .

CMD ["python", "init_postgis.py"]
