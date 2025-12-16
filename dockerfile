FROM apache/airflow:2.10.2-python3.9

USER root
COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
