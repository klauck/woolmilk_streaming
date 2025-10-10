FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    openssh-server \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

RUN mkdir -p /var/run/sshd
RUN ssh-keygen -A

RUN cat > /etc/ssh/sshd_config << 'EOF'
Port 22
PermitRootLogin yes
PasswordAuthentication yes
PubkeyAuthentication no
ChallengeResponseAuthentication no
UsePAM no
PermitEmptyPasswords yes
EOF

# Set empty password for root
RUN passwd -d root

COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt

COPY . /app/woolmilk

RUN cargo install nexmark --features bin

EXPOSE 22

CMD ["/usr/sbin/sshd", "-D"]