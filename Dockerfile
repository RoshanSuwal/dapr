FROM golang:1.22.6 as builder

COPY cmd /dapr/cmd
COPY dapr /dapr/dapr
COPY pkg /dapr/pkg
COPY tools /dapr/tools
COPY utils /dapr/utils

WORKDIR /dapr

COPY go.mod go.mod

RUN go mod tidy
RUN go mod vendor

ENV GO111MODULE=on

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -tags=stablecomponents,unit -a -tags netgo -mod vendor -o ./daprd -x cmd/daprd/main.go


FROM gcr.io/distroless/static:nonroot
COPY --from=builder /dapr/daprd /daprd

ENTRYPOINT ["/daprd"]

#RUN go build -tags=stablecomponents,unit -o daprd -x cmd/daprd/main.go
