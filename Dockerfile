# Stage 1: Build
FROM golang:1.26-alpine AS builder

WORKDIR /build

# Cache dependency downloads before copying source
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-X main.Version=${VERSION} -X main.Commit=${COMMIT} -X main.BuildDate=${BUILD_DATE}" \
    -o /sagegw ./cmd/sagegw

# Stage 2: Runtime
FROM alpine:3.22

RUN apk --no-cache add ca-certificates tzdata && \
    addgroup -S sage && adduser -S sage -G sage

COPY --from=builder /sagegw /sagegw

USER sage
EXPOSE 3069

ENTRYPOINT ["/sagegw"]
