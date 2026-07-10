FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /loadgen .

FROM scratch
COPY --from=build /loadgen /loadgen
ENTRYPOINT ["/loadgen"]
