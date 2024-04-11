FROM gzettar.ufla.br/airflow/airflow-dev:1.0

USER root
ENV TZ=America/Sao_Paulo
RUN apt-get install wget unzip axel libcurl4-gnutls-dev librtmp-dev -y
COPY ./requirements.txt ./requirements.txt

USER airflow
RUN pip install -r requirements.txt