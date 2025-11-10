FROM python:3.13

# System deps for GeoPandas, GDAL, PostGIS, FastAPI, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin libgdal-dev \
    libproj-dev proj-data proj-bin \
    libgeos-dev \
    libpq-dev \
    curl \
    ca-certificates \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# Hint GDAL/PROJ locations to Python packages that expect them
ENV GDAL_DATA=/usr/share/gdal/3.9
ENV PROJ_LIB=/usr/share/proj
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline + worker + util scripts
COPY fim.py nwm_fim_worker.py run_single_t0.py run_range.py geoserver_init.py ./

# Default: run the FastAPI worker

CMD ["uvicorn", "nwm_fim_worker:app", "--host", "0.0.0.0", "--port", "8000"]
