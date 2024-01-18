FROM continuumio/miniconda3:master-alpine

RUN apk --update add openjdk8-jre

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .
RUN pip install .

RUN pytest --testdox
