FROM python:3

WORKDIR /home/edgenode

ADD . .

RUN pip install -r requirements.txt

CMD [ "python", "./telegrambot.py" ]