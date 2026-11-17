FROM golang:1.26.4-bookworm AS build
ENV GOTOOLCHAIN=local
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY fixtures ./fixtures
RUN CGO_ENABLED=0 go build -trimpath -o /tc-cache .

FROM scratch AS runtime
COPY --from=build /tc-cache /tc-cache
ENTRYPOINT ["/tc-cache"]
