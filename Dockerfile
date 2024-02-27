# base docker image
FROM python:3.8-slim-buster

# install pip
RUN apt-get update && \
	apt-get install -y python3-pip && \
	ln -sfn /usr/bin/python3.7 /usr/bin/python && \
	ln -sfn /usr/bin/pip3 /usr/bin/pip

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN deps='build-essential gdal-bin python-gdal libgdal-dev kmod wget apache2' && \
	apt-get update && \
	apt-get install -y $deps

RUN pip install --upgrade pip && \
	pip install GDAL==$(gdal-config --version)

# install drought_model
WORKDIR /drought_model
ADD drought_model .
RUN pip install .
