from python:alpine
RUN apk add py3-pip py3-cryptography py3-pandas python-dev
RUN mkdir /bot
RUN mkdir /bot/source
RUN mkdir /data
COPY source/* /bot/source/
RUN pip3 install -r /bot/source/requirements.txt
WORKDIR /data/
CMD [ "python3", "/bot/source/bot.py" ]
