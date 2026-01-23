# --- Stage 1: Build ---
FROM golang:1.24-alpine AS builder

# Install git for downloading dependencies
RUN apk add --no-cache git

WORKDIR /app

# 1. Copy go.mod and go.sum from the root to cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# 2. Copy the entire project to access internal/proto and internal/service
COPY . .

# 3. Build the Stream Service binary from its specific main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o stream-service ./cmd/stream/main.go

# --- Stage 2: Runtime ---
FROM alpine:latest

RUN apk add --no-cache ca-certificates

# Security: Run as non-root user
RUN adduser -D appuser
USER appuser

WORKDIR /app

# Copy only the compiled binary
COPY --from=builder /app/stream-service .

# 8080: Discovery HTTP API | 50051: Ingest gRPC
EXPOSE 8080 50051

CMD ["./stream-service"]