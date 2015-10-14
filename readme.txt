Statsd-router

Statsd is a convenient way to collect instrumentation data.  Unfortunately,
statsd doesn't support clustering out of the box. Clustering is necessary to
process each metric by a single statsd instance. If two statsd instances push
data for the same metric it causes data corruption.

Statsd-router is designed to run behind a load balancer. Each statsd instance
is aware how many statsd-router instances are in the cluster and what their
health statuses are. Each metric is routed to a single statsd instance based on
consistent hashing. If one statsd instance goes down, metrics from this
instance are spread among alive instances; distribution of other metrics
wouldn't be affected.

Statsd-router does the following:

1. accepts statsd-compatible data via UDP, parses it, aggregates metrics with
   certain statsd instance via consistent hashing
2. flushes data to statsd instances when the aggregated data size approaches
   MTU, or on a scheduled basis, whatever comes first
3. provides health status to external parties to confirm that statsd-router
   itself is alive
4. check health status of underlying statsd instances to eliminate dead ones
   from consistent hashing

Statsd-router is implemented using the libev library. It is a single-threaded 
application built using non-blocking IO.

Testing.

Tests are located in test/ directory. You can run them either via
'make test' command or by running run-all-tests.sh in test/ directory.

Each test can be run individually. You can provide -v option to get
verbose output.

There is also an old (and currently broken) functional test called
statsd-router-monkey.rb.  It starts statsd-router and simulates several statsd
instances. Those instances are periodically started and stopped.
statsd-router-monkey.rb checks that metrics are delivered and that they are
delivered to the correct statsd instance. Any unexpected behavior errors are
logged.
