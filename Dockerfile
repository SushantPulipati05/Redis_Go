FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /redis-go ./cmd/redis-go

FROM alpine:3.20
COPY --from=build /redis-go /usr/local/bin/redis-go
WORKDIR /data
VOLUME /data
EXPOSE 6380
ENTRYPOINT ["redis-go"]
