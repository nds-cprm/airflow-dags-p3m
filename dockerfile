FROM apache/airflow:2.7.1-python3.9

USER root
ENV TZ=America/Sao_Paulo

RUN apt-get update && \
    apt-get install -y \
        wget \
        unzip \
        axel \
        libcurl4-gnutls-dev \
        librtmp-dev \
        gdal-bin \
        libgdal-dev \
        python3-gdal && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
