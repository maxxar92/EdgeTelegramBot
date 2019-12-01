FROM python:3

WORKDIR /home/edgenode

ADD ./requirements.txt .

RUN apt-get update && apt-get install -y libgeos-dev libgdal-dev

RUN pip install -r requirements.txt

ADD . .

CMD [ "python", "./telegrambot.py" ]
