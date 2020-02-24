FROM python:3.6-slim

WORKDIR /servo

# replaced 'latest stable' with fixed version of kubectl, to support k8s 1.8.x
#    ver=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt) ; 
# hadolint ignore=DL3013
RUN set -e ; apt-get update ; apt-get dist-upgrade -y ; apt-get install --no-install-recommends -y curl ; \
    ver=v1.10.3 ; \
    curl -LO https://storage.googleapis.com/kubernetes-release/release/$ver/bin/linux/amd64/kubectl ; \
    chmod +x ./kubectl ; mv ./kubectl /usr/local/bin/kubectl ; \
    pip3 install requests PyYAML ; \
    apt-get clean ; \
    rm -rf /var/lib/apt/lists/*

ADD servo-m w2.py /servo/

CMD [ "/bin/sh", "/servo/servo-m" ]
