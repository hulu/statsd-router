#ifndef _SR_TYPES_H
#define _SR_TYPES_H

#include <ev.h>
#include <netinet/in.h>

// extended ev structure with id field
// used to check downstream health
struct ev_io_id {
    struct ev_io super;
    int id;
};

// extended ev structure with buffer pointer and buffer length
// used by control port connections
struct ev_io_control {
    struct ev_io super;
    char *buffer;
    int buffer_length;
};

// Size of buffer for outgoing packets. Should be below MTU.
// TODO Probably should be configured via configuration file?
#define DOWNSTREAM_BUF_SIZE 1450
#define DOWNSTREAM_BUF_NUM 1024
#define METRIC_SIZE 256

// structure that holds downstream data
struct downstream_s {
    // buffer where data is added
    int active_buffer_idx;
    char *active_buffer;
    int active_buffer_length;
    // buffer ready for flush
    int flush_buffer_idx;
    // memory for active and flush buffers
    char buffer[DOWNSTREAM_BUF_SIZE * DOWNSTREAM_BUF_NUM];
    // lengths of buffers from the above array
    int buffer_length[DOWNSTREAM_BUF_NUM];
    // sockaddr for data
    struct sockaddr_in sa_in_data;
    // sockaddr for health
    struct sockaddr_in sa_in_health;
    // id extended ev_io structure used for downstream health checks
    struct ev_io_id health_watcher;
    // id extended ev_io structure used for sending data to downstream
    struct ev_io_id flush_watcher;
    // last time data was flushed to downstream
    ev_tstamp last_flush_time;
    // each statsd instance during each ping interval
    // would increment per connection counters
    // this would allow us to detect metrics loss and locate
    // statsd-router to statsd connection with data loss
    char per_downstream_counter_metric[METRIC_SIZE];
    int per_downstream_counter_metric_length;
    // metrics to detect downstreams with highest traffic
    char downstream_traffic_counter_metric[METRIC_SIZE];
    int downstream_traffic_counter;
    char downstream_packet_counter_metric[METRIC_SIZE];
    int downstream_packet_counter;
    // bit flag if this downstream is alive
    unsigned int alive:1;
};

#define HEALTH_CHECK_REQUEST "health"
#define HEALTH_CHECK_RESPONSE_BUF_SIZE 32
#define HEALTH_CHECK_UP_RESPONSE "health: up\n"

// statsd-router ports are stored in array and accessed using indexes below
#define DATA_PORT_INDEX 0
#define CONTROL_PORT_INDEX 1

// number of ports used by statsd-router
#define PORTS_NUM 2

// globally accessed structure with commonly used data
struct global_s {
    // statsd-router ports, accessed via DATA_PORT_INDEX and CONTROL_PORT_INDEX
    int port[PORTS_NUM];
    // how many downstreams we have
    int downstream_num;
    // array of downstreams
    struct downstream_s *downstream;
    // how often we check downstream health
    ev_tstamp downstream_health_check_interval;
    // how often we flush data
    ev_tstamp downstream_flush_interval;
    // how noisy is our log
    int log_level;
    // how often we want to send ping metrics
    ev_tstamp downstream_ping_interval;
    char alive_downstream_metric_name[METRIC_SIZE];
    char health_check_response_buf[HEALTH_CHECK_RESPONSE_BUF_SIZE];
    int health_check_response_buf_length;
};

struct global_s global;

#endif
