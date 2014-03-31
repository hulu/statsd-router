all:
	gcc -O2 -o statsd-router statsd-router.c -lev
clean:
	rm statsd-router
