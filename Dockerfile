# Stage 1: Build
#
# --platform=$BUILDPLATFORM: the builder always runs natively and Go
# cross-compiles for the target. Without it, a multi-platform buildx build
# runs this whole stage under QEMU for the foreign architecture — a Go
# compile of the shannon-sdk dependency tree under emulation takes the
# better part of an hour; cross-compiled it takes minutes.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

WORKDIR /build

# Cache dependency downloads before copying source
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
# Set by buildx per target platform; default to the builder's own for a plain
# `docker build`.
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags "-X main.Version=${VERSION} -X main.Commit=${COMMIT} -X main.BuildDate=${BUILD_DATE}" \
    -o /sagegw ./cmd/sagegw

# Stage 2: Runtime
FROM alpine:3.24

RUN apk --no-cache add ca-certificates tzdata && \
    addgroup -S sage && adduser -S sage -G sage

COPY --from=builder /sagegw /sagegw

USER sage
EXPOSE 3069

ENTRYPOINT ["/sagegw"]
