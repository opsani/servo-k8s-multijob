# hadolint ignore=DL3007
FROM busybox:latest

ENTRYPOINT [ "/bin/sh", "-c", "trap '' TERM ; read r </dev/ptmx ;" ]
