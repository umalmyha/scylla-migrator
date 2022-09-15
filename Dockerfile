FROM golang:1.19-alpine AS build

WORKDIR /migrate

COPY . .

RUN go mod download
RUN CGO_ENABLED=0 go build -o /migrator .

FROM alpine:latest

COPY --from=build /migrator /migrator

VOLUME migrations

CMD ["/migrator"]