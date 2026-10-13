FROM golang:1.27.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /relay ./cmd/relay

FROM alpine:3.22
RUN adduser -D -u 10001 relay
USER relay
COPY --from=build /relay /usr/local/bin/relay
EXPOSE 8080
ENTRYPOINT ["relay"]
CMD ["api"]
