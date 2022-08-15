FROM python:slim-buster

WORKDIR /app

COPY . /app

RUN apt-get update 
RUN apt-get -y install wget 
RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "ingest_data.py"]

