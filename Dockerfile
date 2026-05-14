FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o bin/oml-aggr-mcp ./cmd/mcp-hub
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o bin/mcp-stdio ./cmd/mcp-stdio

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app

COPY --from=builder /app/bin/oml-aggr-mcp /app/oml-aggr-mcp
COPY --from=builder /app/bin/mcp-stdio /app/mcp-stdio

EXPOSE 3000
EXPOSE 8081

CMD ["./oml-aggr-mcp"]
