FROM apache/airflow:2.7.1-python3.9

ENV TZ=America/Sao_Paulo

USER root

RUN sh -c echo "deb http://apt.postgresql.org/pub/repos/apt/ buster-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN sh -c echo "deb http://deb.debian.org/debian/ stable main contrib non-free" > /etc/apt/sources.list.d/debian.list

RUN sh -c echo "deb http://deb.debian.org/debian/ bullseye main contrib non-free" > /etc/apt/sources.list.d/debian.list

RUN apt upgrade -y && apt update -y

RUN apt-get install -y apt-utils software-properties-common wget

RUN sh -c echo "deb https://ppa.launchpadcontent.net/ubuntugis/ubuntugis-unstable/ubuntu xenial main " > /etc/apt/sources.list.d/debian.list

RUN apt update -y

RUN apt-get upgrade -y && apt-get update -y

RUN apt-get update -y 

RUN apt-get clean && apt-get install -y  --allow-downgrades\
    build-essential gdal-bin libgdal-dev libpq5=13.11-0+deb11u1 libpq-dev \
    libxml2-dev libxml2 libxslt1-dev zlib1g-dev libjpeg-dev \
    gcc python3-gdal libmemcached-dev libldap2-dev libsasl2-dev libffi-dev

RUN apt-get install -y gzip unzip

RUN apt-get clean

COPY ./requirements.txt ./requirements.txt

USER airflow

RUN pip install pip --upgrade

RUN pip install pygdal==$(gdal-config --version).*

RUN pip install -r requirements.txt