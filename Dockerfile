FROM python:3.6-slim

WORKDIR /servo

RUN set -e ; apt-get update ; apt-get dist-upgrade -y ; apt-get install -y curl ; \
    ver=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt) ; \
    curl -LO https://storage.googleapis.com/kubernetes-release/release/$ver/bin/linux/amd64/kubectl ; \
    chmod +x ./kubectl ; mv ./kubectl /usr/local/bin/kubectl ; \
    pip3 install requests PyYAML

ADD servo-m w2.py /servo/

CMD [ "/bin/sh", "/servo/servo-m" ]
