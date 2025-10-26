# Gunakan base image resmi Airflow versi 3.1.0
FROM apache/airflow:3.1.0

# Set work directory di dalam container
WORKDIR /opt/airflow

# Copy file requirements.txt dari lokal ke dalam container
COPY requirements.txt .

# Install dependensi tambahan yang diperlukan untuk DAG
RUN pip install --no-cache-dir -r requirements.txt
