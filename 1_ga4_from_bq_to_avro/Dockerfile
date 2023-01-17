FROM python:3.8-slim-buster

RUN apt-get update \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV LANG ja_JP.UTF-8 \
    LANGUAGE ja_JP:ja \
    LC_ALL ja_JP.UTF-8 \
    TZ JST-9 \
    TERM xterm
ENV PYTHONUNBUFFERED 1

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools

RUN mkdir -p /script /workdir
WORKDIR /workdir
VOLUME ["/workdir"]

ENV GOOGLE_APPLICATION_CREDENTIALS /workdir/service_account.json

COPY ./deploy/* /script/
RUN pip install --no-cache-dir -r /script/requirements.txt
CMD [ "python", "/script/ga4_from_bq_to_avro.py" ]
