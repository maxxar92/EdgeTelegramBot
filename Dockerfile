FROM python:3

WORKDIR /home/edgenode

ADD ./requirements.txt .

RUN pip install -r requirements.txt

ADD . .

CMD [ "python", "./telegrambot.py" ]
