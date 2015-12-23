/*
 * This is statsd-router: metrics router for statsd cluster.
 *
 * Statsd (https://github.com/etsy/statsd/) is a very convenient way for collecting metrics.
 * Statsd-router can be used to scale statsd. It accepts metrics and routes them across
 * several statsd instances in such way, that each metric is processed by one and the
 * same statsd instance. This is done in order not to corrupt data while using graphite
 * as backend.
 *
 * Author: Kirill Timofeev <kvt@hulu.com>
 *	
 * Enjoy :-)!	
 *	
 */

#pragma GCC diagnostic ignored "-Wstrict-aliasing"

#ifndef _SR_MAIN_H
#define _SR_MAIN_H

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <netinet/in.h>
#include <ev.h>
#include <netdb.h>
#include <sys/fcntl.h>
#include <time.h>
#include <signal.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>

#include "sr-util.h"
#include "sr-types.h"

#define STRLEN(s) (sizeof(s) / sizeof(s[0]) - 1)

#define HEALTHY_DOWNSTREAMS "healthy_downstreams"
#define PER_DOWNSTREAM_COUNTER_METRIC_SUFFIX "connections:1|c"
#define DOWNSTREAM_PACKET_COUNTER "packets"
#define DOWNSTREAM_TRAFFIC_COUNTER "traffic"

// Size of other temporary buffers
#define DATA_BUF_SIZE 4096
#define DOWNSTREAM_HEALTH_CHECK_BUF_SIZE 32
#define CONTROL_REQUEST_BUF_SIZE 32
#define LOG_BUF_SIZE 2048

// 3 last bytes of the metric name hash are used to do preliminary filtering
// those 3 bytes are used as an index and appropriate counter is incremented
// if counter value is over threshold metric is checked for excessive rate
#define HASH_MASK 0xffffff

// this structure is shared by all statsd-router processes
struct shared_s {
    // rough rate counters, partitioning is done using last 3 bytes of metric name hash
    unsigned int rate_counter[HASH_MASK];
};

int init_config(char *filename);
void control_accept_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);
void ds_health_check_timer_cb(struct ev_loop *loop, struct ev_periodic *p, int revents);

#endif
