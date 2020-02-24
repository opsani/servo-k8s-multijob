FROM python:3.6-alpine

WORKDIR /servo

# hadolint ignore=DL3013
RUN set -e ; pip3 install requests

ADD pod-mon.py /servo/

CMD [ "/servo/pod-mon.py" ]
