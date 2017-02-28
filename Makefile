GOPATH:=$(GOPATH):$(PWD)
BIN=bin
EXE=influxdb-collectd-proxy
GOCOLLECTD=github.com/paulhammond/gocollectd
INFLUXDBGO=github.com/influxdata/influxdb/client/v2
all: clean get build 
build:
	GOOS=linux GOARCH=amd64 go build -o $(BIN)/$(EXE) influxdb-collectd-proxy.go  typesdb.go

get:
	GOPATH=$(GOPATH) go get $(GOCOLLECTD)
	GOPATH=$(GOPATH) go get $(INFLUXDBGO)

clean: 
	rm -rf  $(BIN)/$(EXE) 


