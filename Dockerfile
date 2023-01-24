#from archlinux
from docker.io/armv7/armhf-archlinux
run pacman --noconfirm -Sy python python-pip python-matplotlib python-pandas python-sqlalchemy
RUN mkdir /bot
RUN mkdir /bot/source
RUN mkdir /data
COPY source/* /bot/source/
RUN pip3 install -r /bot/source/requirements.txt
WORKDIR /data/
CMD [ "python3", "/bot/source/bot.py" ]
