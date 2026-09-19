# syntax=docker/dockerfile:1

# Stage 1: Build binaries
FROM golang:alpine AS builder

ENV GOTOOLCHAIN=auto

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Target component build argument (api or worker)
ARG TARGET=api

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/bin/service ./cmd/${TARGET}

# Stage 2: Minimal runtime image
FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S flowforge && adduser -S flowforge -G flowforge

COPY --from=builder /app/bin/service /app/service

USER flowforge:flowforge

EXPOSE 8080

ENTRYPOINT ["/app/service"]
