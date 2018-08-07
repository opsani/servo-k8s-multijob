FROM python:3.6-alpine

WORKDIR /servo

RUN set -e ; apk update ; apk upgrade ; \
    pip3 install requests

ADD pod-mon.py /servo/

CMD [ "/servo/pod-mon.py" ]
