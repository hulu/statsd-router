PKG_NAME=hulu-statsd-router
PKG_VERSION=0.0.12
PKG_DESCRIPTION="Metrics router for statsd cluster"

CC=gcc
CFLAGS=-c -Wall -O2
LDFLAGS=-lev -lpthread
SOURCES=sr-control-server.c sr-health-client.c sr-init.c sr-main.c sr-util.c
OBJECTS=$(SOURCES:.c=.o)
EXECUTABLE=statsd-router

.PHONY: all test clean

all: $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS) 
	$(CC) $(OBJECTS) -o $@ $(LDFLAGS)
.c.o:
	$(CC) $(CFLAGS) $< -o $@
clean:
	rm -rf statsd-router *.o build
pkg: all
	mkdir -p build/usr/local/bin/
	cp statsd-router build/usr/local/bin/
	cd build && \
	fpm --deb-no-default-config-files --deb-user root --deb-group root -d libev-dev --description $(PKG_DESCRIPTION) -s dir -t deb -v $(PKG_VERSION) -n $(PKG_NAME) `find usr -type f` && \
	rm -rf `ls|grep -v deb$$`
test:
	cd test && ./run-all-tests.sh
