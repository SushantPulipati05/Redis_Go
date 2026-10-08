# ---- build stage: compile a static binary ----
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /redis-go .

# ---- run stage: tiny image with just the binary ----
FROM alpine:3.20
COPY --from=build /redis-go /usr/local/bin/redis-go
WORKDIR /data
VOLUME /data
EXPOSE 6380
ENTRYPOINT ["redis-go"]
