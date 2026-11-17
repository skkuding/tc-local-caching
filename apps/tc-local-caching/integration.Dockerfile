# Build from repository root. No shared Go module is introduced.
FROM golang:1.26.4-bookworm AS build
ENV GOTOOLCHAIN=local
WORKDIR /src/manager
COPY apps/tc-local-caching/go.mod apps/tc-local-caching/go.sum ./
RUN go mod download
COPY apps/tc-local-caching/ ./
RUN go build -trimpath -o /out/tc-cache . && go test -race -c -o /out/manager.test
WORKDIR /src/iris
COPY apps/iris/go.mod apps/iris/go.sum ./
RUN go mod download
COPY apps/iris/ ./
RUN go test -race -c -o /out/iris-tc.test ./src/service/testcase \
 && go test -race -c -o /out/iris-handler.test ./src/handler

FROM golang:1.26.4-bookworm AS verify
ARG TARGETARCH
RUN curl -fL --retry 3 "https://github.com/skkuding/Judger/releases/download/v1.0.0-alpha.4/libjudger-${TARGETARCH}.so" -o /usr/local/bin/libjudger \
 && chmod 755 /usr/local/bin/libjudger
COPY --from=build /out/ /usr/local/bin/
COPY apps/tc-local-caching/verify.sh /usr/local/bin/verify
ENV TC_MANAGER_BINARY=/usr/local/bin/tc-cache \
    LIBJUDGER_PATH=/usr/local/bin/libjudger \
    TC_CAPACITY_ROOT=/test-cache
ENTRYPOINT ["bash", "/usr/local/bin/verify"]
