FROM ubuntu:20.04

RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev \
    build-essential libssl-dev libffi-dev python3-setuptools

RUN pip install requests

ENV PYTHONPATH="/usr/local:$PYTHONPATH"

WORKDIR /usr/local/druid

CMD ["tail", "-f", "/bin/bash"]
