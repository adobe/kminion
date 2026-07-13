############################################################
# Build image
############################################################
FROM golang:1.26.5-alpine AS builder

ARG VERSION
ARG BUILT_AT
ARG COMMIT

RUN apk update && apk upgrade --no-cache && apk add --no-cache git ca-certificates && update-ca-certificates

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build \
    -ldflags="-w -s \
    -X main.version=$VERSION \
    -X main.commit=$COMMIT \
    -X main.builtAt=$BUILT_AT" \
    -o ./bin/kminion

############################################################
# Runtime Image
############################################################
FROM alpine:3.24
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/bin/kminion /app/kminion
RUN chmod -R +x /app/kminion

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/ready || exit 1

ENTRYPOINT ["/app/kminion"]
