Statsd-router

To collect instrumentation data it is convenient to use statsd. Unfortunately
statsd doesn't support clustering out of the box. Clustering is necessary to
process each metrics by single statsd instance. If 2 statsd instances would
push data for the same metric data would be corrupted.

Statsd-router is supposed to be used as backends of load balancer. Each statsd
instance is aware how many statsds are in the cluster and what is their health
status. Each metric is send to single statsd instance based on consistent
hashing. If 1 statsd instance would go down metrics from this instance would
be spread among alive instances, distribution of other metrics wouldn't be
affected.

Statsd-router does the following:

1. accepts statsd compatible data via udp, parses it, aggregates metrics with
   certain statsd instance via consistent hashing
2. flushes data to statsd instances when aggregated data size approaches mtu
   or on scheduled basis, whatever comes first
3. provides health status to external parties to confirm that statsd-router
   itself is alive
4. check health status of underlying statsd instances to eliminate dead ones
   from cosistent hashing

Statsd-router is implemente using libev library. It is single threaded
application build using non-blocking IO. 

Testing.

There is functional test called statsd-router-monkey.rb. After building
new statsd-router version you can check if it behaves as expected by
running statsd-router-monkey.rb. It starts statsd-router and simulates
several statsd instances. Those instances are periodically started and 
stopped. statsd-router-monkey.rb checks that metrics are delivered and
that they are delivered to correct statsd instance. In case of any
unexpected behavior errors would be logged.
