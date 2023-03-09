# syntax=docker/dockerfile:1

FROM golang:1.16

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY *.go ./

RUN go build -o /kvs_server

EXPOSE 8090

CMD ["/kvs_server"]
