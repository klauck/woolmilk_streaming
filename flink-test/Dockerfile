FROM flink:1.20.0

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev openjdk-11-jdk-headless && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    JHOME="$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" && \
    ln -sf "$JHOME/include" /opt/java/openjdk/include && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.20.0
