FROM alpine:3.22

# Installieren der notwendigen Pakete
RUN apk update && apk add --no-cache \
    git \
    python3 \
    py3-pip \
    py3-matplotlib \
    py3-pandas \
    cython \
    py3-scikit-learn
    
# Erstellen der Verzeichnisse
RUN mkdir /bot
RUN mkdir /bot/source
RUN mkdir /data

# Kopieren der Quelldateien
COPY source/* /bot/source/

# Erstellen einer virtuellen Umgebung
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv --system-site-packages $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN apk update && apk add --no-cache --virtual .build-deps \
 g++ \
 make \
 cmake \
 llvm \
 llvm-dev \
 musl-dev \
 libxml2-dev \
 libxslt-dev \
 python3-dev

RUN pip3 install --no-cache-dir -r /bot/source/requirements.txt
RUN pip3 install --no-cache-dir git+https://github.com/ranaroussi/yfinance.git@dev
#RUN pip3 install --no-cache-dir llvmlite==0.45.0
#RUN pip3 install --no-cache-dir pandas-ta
RUN apk del .build-deps

# Wechsel in das Arbeitsverzeichnis
WORKDIR /data/

# Ausführen des Bots
CMD ["python3", "/bot/source/bot.py"]
