from docker.io/manjarolinux/base
run pacman --noconfirm -Syu git python python-pip python-matplotlib python-pandas python-appdirs python-logbook python-cffi python-pyrsistent python-pycryptodome python-aiohttp python-future libxml2 libxslt gcc python-wheel cython python-scikit-learn
RUN mkdir /bot
RUN mkdir /bot/source
RUN mkdir /data
COPY source/* /bot/source/
RUN python3 -m venv --system-site-packages /opt/venv
RUN /opt/venv/bin/pip3 install -r /bot/source/requirements.txt
RUN /opt/venv/bin/pip3 install git+https://github.com/ranaroussi/yfinance.git@dev
WORKDIR /data/
CMD [ "/opt/venv/bin/python3", "/bot/source/bot.py" ]
