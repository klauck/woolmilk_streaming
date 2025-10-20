FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    openssh-server \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

RUN useradd -m -s /bin/bash picocluster

RUN mkdir -p /var/run/sshd
RUN ssh-keygen -A

COPY id_rsa.pub /tmp/host_key.pub

RUN mkdir -p /home/picocluster/.ssh && \
    cat /tmp/host_key.pub > /home/picocluster/.ssh/authorized_keys && \
    rm /tmp/host_key.pub && \
    chown -R picocluster:picocluster /home/picocluster/.ssh && \
    chmod 700 /home/picocluster/.ssh && \
    chmod 600 /home/picocluster/.ssh/authorized_keys

RUN mkdir -p /etc/ssh/sshd_config.d && \
    printf "%s\n" \
      "Port 22" \
      "PermitRootLogin no" \
      "PasswordAuthentication no" \
      "PubkeyAuthentication yes" \
      "AuthorizedKeysFile .ssh/authorized_keys" \
      "AllowUsers picocluster" \
      "ChallengeResponseAuthentication no" \
      "UsePAM yes" \
      "HostKey /etc/ssh/ssh_host_rsa_key" \
      "HostKey /etc/ssh/ssh_host_ecdsa_key" \
      "HostKey /etc/ssh/ssh_host_ed25519_key" \
      "StrictModes yes" \
    > /etc/ssh/sshd_config.d/picocluster.conf

# set again the permissions every time container starts
RUN echo '#!/bin/bash\n\
set -e\n\
echo "Setting up SSH permissions..."\n\
chown -R picocluster:picocluster /home/picocluster/.ssh\n\
chmod 700 /home/picocluster/.ssh\n\
chmod 600 /home/picocluster/.ssh/authorized_keys\n\
echo "Starting SSH daemon..."\n\
/usr/sbin/sshd -D\n' > /start-sshd.sh && chmod +x /start-sshd.sh


COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt

COPY . /home/picocluster/woolmilk
RUN chown -R picocluster:picocluster /home/picocluster/woolmilk

RUN cargo install nexmark --features bin && \
    cp /root/.cargo/bin/nexmark /usr/local/bin/nexmark && \
    chmod +x /usr/local/bin/nexmark

EXPOSE 22
CMD ["/start-sshd.sh"]