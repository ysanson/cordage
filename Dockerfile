# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /cordage ./cmd/cordage

FROM gcr.io/distroless/static-debian12
COPY --from=builder /cordage /cordage
ENTRYPOINT ["/cordage"]
