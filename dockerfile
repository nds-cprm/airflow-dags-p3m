ARG AIRFLOW_VERSION=2.11.2
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

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
        build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install "gdal==$(gdal-config --version).*" --global-option=build_ext --global-option="-I/usr/include/gdal" --global-option="-L/usr/lib" 