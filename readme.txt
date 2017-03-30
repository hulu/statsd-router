Statsd-router

To collect instrumentation data, it is convenient to use statsd. Unfortunately,
statsd doesn't support clustering out of the box. Clustering is necessary to
process each metric by a single statsd instance. If 2 statsd instances were to
push data for the same metric, there would be data corruption.

Statsd-router is supposed to be used as a backend by the load balancer. Each statsd
instance is aware how many statsds are in the cluster and what their health
statuses are. Each metric is sent to a single statsd instance based on consistent
hashing. If 1 statsd instance goes down, metrics from this instance
are spread among alive instances; distribution of other metrics wouldn't be
affected.

Statsd-router does the following:

1. accepts statsd-compatible data via UDP, parses it, aggregates metrics with
   certain statsd instance via consistent hashing
2. flushes data to statsd instances when the aggregated data size approaches
   MTU, or on a scheduled basis, whatever comes first
3. provides health status to external parties to confirm that statsd-router
   itself is alive
4. check health status of underlying statsd instances to eliminate dead ones
   from consistent hashing

Statsd-router is implemented using the libev library and uses non-blocking IO.

Configuration file.

Following configuration parameters are supported:

data_port - base udp port to accept incoming data. Thread 0 will use data_port, thread 1 will use data_port + 1 etc.
control_port - tcp port for health check
downstream_flush_interval - how often we flush data to the downstreams, seconds
downstream_health_check_interval - how often we check downstream health, seconds
downstream_ping_interval - how often we send ping metrics
ping_prefix - prefix used for the ping metrics
downstream - comma separated list of the downstreams. Each downstream has format address:data_port:health_port
log_level - 0: TRACE, 1: DEBUG, 2: INFO, 3: WARN, 4: ERROR
threads_num - how many threads will be used
socket_out_num - how many outgoing sockets we use for the downstreams

Testing.

Tests are located in test/ directory. You can run them either via
'make test' command or by running run-all-tests.sh in test/ directory.

Each test can be run individually. You can provide -v option to get
verbose output.

There is also old (and currently broken) functional test called
statsd-router-monkey.rb.  It starts statsd-router and simulates
several statsd instances. Those instances are periodically started and 
stopped. statsd-router-monkey.rb checks that metrics are delivered and
that they are delivered to the correct statsd instance. Any unexpected
behavior errors are logged.
