#  Stage 1: dependencies 
FROM golang:1.26-alpine AS deps

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

#  Stage 2: builder 
FROM deps AS builder

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -ldflags="-w -s -extldflags '-static'" \
    -trimpath \
    -o /bin/realtime \
    ./cmd/realtime

#  Stage 3: dev 
FROM deps AS dev

RUN go install github.com/air-verse/air@latest

WORKDIR /app
COPY . .

EXPOSE 8085
CMD ["air", "-c", ".air.toml"]

#  Stage 4: production 
FROM alpine:3.22.4 AS production

RUN apk add --no-cache ca-certificates tzdata wget \
    && addgroup -S appgroup && adduser -S appuser -G appgroup

COPY --from=builder /bin/realtime /realtime

USER appuser:appgroup

EXPOSE 8085

ENTRYPOINT ["/realtime"]