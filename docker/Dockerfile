FROM python:3.8

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app

RUN chmod +x /code/app/start_scripts/start_app.sh

CMD ["/code/app/start_scripts/start_app.sh"]
