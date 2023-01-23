from python:alpine
RUN apk add py3-pip python3-dev py3-cryptography py3-pandas py3-numpy gcc
RUN mkdir /bot
RUN mkdir /bot/source
RUN mkdir /data
COPY source/* /bot/source/
RUN pip3 install -r /bot/source/requirements.txt
WORKDIR /data/
CMD [ "python3", "/bot/source/bot.py" ]
