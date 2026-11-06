FROM golang:1.24-alpine AS build

WORKDIR /src

# As dependências vão em uma camada separada para o cache do Docker não ser
# invalidado a cada mudança de código.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/vrsync ./cmd/vrsync

FROM alpine:3.20

# tzdata para a agenda da carga completa respeitar o TZ configurado.
RUN apk add --no-cache tzdata && adduser -D -u 10001 vrsync
COPY --from=build /out/vrsync /usr/local/bin/vrsync

USER vrsync
EXPOSE 28299

ENTRYPOINT ["/usr/local/bin/vrsync"]
