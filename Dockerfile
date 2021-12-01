FROM python:3.9-slim

RUN apt-get update -y && apt-get upgrade -y && apt-get install ffmpeg libsm6 libxext6  -y


COPY config.toml /
COPY . /app

WORKDIR /app

ADD requirements.txt /app/requirements.txt

RUN pip install -U pip && pip install -r requirements.txt

EXPOSE 8502
ENV PYTHONPATH "${PYTHONPATH}:/app/common-code"


ENTRYPOINT ["uvicorn", "--host=0.0.0.0", "app:app", "--port=8502", "--app-dir=./fetcher", "--root-path=api"]

