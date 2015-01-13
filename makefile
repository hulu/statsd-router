PKG_NAME=hulu-statsd-router
PKG_VERSION=0.0.10
PKG_DESCRIPTION="Metrics router for statsd cluster"

.PHONY: all test clean

all: bin
bin:
	gcc -Wall -O2 -o statsd-router statsd-router.c -lev
clean:
	rm -rf statsd-router build
pkg: bin
	mkdir -p build/usr/local/bin/
	cp statsd-router build/usr/local/bin/
	cd build && \
	fpm --deb-user root --deb-group root -d libev-dev --description $(PKG_DESCRIPTION) -s dir -t deb -v $(PKG_VERSION) -n $(PKG_NAME) `find usr -type f` && \
	rm -rf `ls|grep -v deb$$`
test:
	cd test && ./run-all-tests.sh
