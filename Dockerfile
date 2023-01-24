from python:slim
RUN apt update &&\
    apt install python3-pip python3-dev python3-cryptography python3-pandas python3-numpy
RUN mkdir /bot
RUN mkdir /bot/source
RUN mkdir /data
COPY source/* /bot/source/
RUN pip3 install -r /bot/source/requirements.txt
WORKDIR /data/
CMD [ "python3", "/bot/source/bot.py" ]
