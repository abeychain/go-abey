# Build Gabey in a stock Go builder container
FROM golang:1.22.12-alpine as construction

RUN apk add --no-cache git make gcc musl-dev=1.2.5-r9 linux-headers

ADD . /abey
RUN cd /abey && go mod tidy && make gabey

# Pull Gabey into a second stage deploy alpine container
FROM alpine:3.18.5

RUN apk add --no-cache ca-certificates
COPY --from=construction /abey/build/bin/gabey /usr/local/bin/
CMD ["gabey"]

EXPOSE 8545 8545 9215 9215 30310 30310 30311 30311 30313 30313
ENTRYPOINT ["gabey"]