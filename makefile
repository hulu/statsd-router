all:
	gcc -Wall -Werror -O2 -o statsd-router statsd-router.c -lev
clean:
	rm statsd-router
