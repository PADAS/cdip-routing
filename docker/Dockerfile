FROM ubuntu:20.04

RUN apt-get update && \
    apt-get -y upgrade && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -y \
    python3-pip \
    redis-tools \
    vim \
    && apt-get autoremove


WORKDIR /workspace

ADD requirements.txt /workspace/requirements.txt

RUN pip3 install --upgrade pip

RUN pip3 install -r /workspace/requirements.txt --no-cache-dir

# Add your app files and scripts here
ADD ./app /var/app

WORKDIR /var

RUN chmod +x /var/app/start_scripts/start_app.sh

CMD ["/var/app/start_scripts/start_app.sh"]
