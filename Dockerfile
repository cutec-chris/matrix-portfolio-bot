#from archlinux
from docker.io/agners/archlinuxarm
run sed -i -e 's/#XferCommand = \/usr\/bin\/curl -L -C - -f -o %o %u/XferCommand = \/usr\/bin\/curl -L -C - -f -o %o %u/' /etc/pacman.conf
#run sed -i -e 's/#XferCommand = \/usr\/bin\/wget --passive-ftp -c -O %o %u/XferCommand = \/usr\/bin\/wget --passive-ftp -c -O %o %u/' /etc/pacman.conf
run pacman --noconfirm -Syu git python python-pip python-matplotlib python-pandas python-sqlalchemy python-appdirs python-logbook python-cffi python-pyrsistent python-pycryptodome python-aiohttp python-future libxml2 libxslt libxml2-debug gcc python-wheel cython python-scikit-learn
RUN mkdir /bot
RUN mkdir /bot/source
RUN mkdir /data
COPY source/* /bot/source/
RUN pip3 install -r /bot/source/requirements.txt
RUN pip3 install git+https://github.com/ranaroussi/yfinance.git@dev
WORKDIR /data/
CMD [ "python3", "/bot/source/bot.py" ]
