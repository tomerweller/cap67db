# Build stage
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build with CGO for SQLite
RUN CGO_ENABLED=1 go build -o cap67db ./cmd/server

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates sqlite

WORKDIR /app

COPY --from=builder /app/cap67db .

# Create data directory for SQLite
RUN mkdir -p /data

ENV DATABASE_PATH=/data/cap67.db
ENV PORT=8080

EXPOSE 8080

CMD ["./cap67db"]
