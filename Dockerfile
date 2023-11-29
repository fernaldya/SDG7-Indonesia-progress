# Base image
FROM apache/airflow:latest

COPY requirements.txt .

RUN pip install -r requirements.txt