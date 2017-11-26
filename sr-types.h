#ifndef _SR_TYPES_H
#define _SR_TYPES_H

#include <ev.h>
#include <netinet/in.h>

// extended ev structure with buffer pointer and buffer length
// used by control port connections
struct ev_io_control {
    struct ev_io super;
    char *response;
    int response_len;
    char *health_response;
    int *health_response_len;;
};

struct ds_health_client_s {
    // ev_io structure used for downstream health checks
    struct ev_io super;
    // sockaddr for health connection
    struct sockaddr_in sa_in;
    // downstream numeric id
    int id;
    // bit flag if this downstream is alive
    unsigned int alive:1;
};

// Size of buffer for outgoing packets. Should be below MTU.
// TODO Probably should be configured via configuration file?
#define DOWNSTREAM_BUF_SIZE 1450
#define DOWNSTREAM_BUF_NUM 1024
#define METRIC_SIZE 256

struct ev_io_ds_s;

struct downstream_s {
    struct ev_io flush_watcher;
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
    // last time data was flushed to downstream
    ev_tstamp last_flush_time;
    // metrics to detect downstreams with highest traffic
    char downstream_traffic_counter_metric[METRIC_SIZE];
    int downstream_traffic_counter;
    char downstream_packet_counter_metric[METRIC_SIZE];
    int downstream_packet_counter;
    // each statsd instance during each ping interval
    // would increment per connection counters
    // this would allow us to detect metrics loss and locate
    // statsd-router to statsd connection with data loss
    char per_downstream_counter_metric[METRIC_SIZE];
    int per_downstream_counter_metric_length;
    struct ds_health_client_s *health_client;
    int *socket_out;
};

struct ev_periodic_health_client_s {
    struct ev_periodic super;
    int downstream_num;
    struct ds_health_client_s *health_client;
};

struct ev_periodic_ds_s {
    struct ev_periodic super;
    int downstream_num;
    struct downstream_s *downstream;
    char *string;
};

struct ev_io_ds_s {
    struct ev_io super;
    int downstream_num;
    struct downstream_s *downstream;
    int socket_in;
};

struct thread_config_s {
    int index;
    pthread_t thread;
    struct sr_config_s *common;
    int socket_in;
    int *socket_out;
    char alive_downstream_metric_name[METRIC_SIZE];
};

#define HEALTH_CHECK_REQUEST "health"
#define HEALTH_CHECK_RESPONSE_BUF_SIZE 32
#define HEALTH_CHECK_UP_RESPONSE "health: up\n"

struct sr_config_s {
    // data port is used to recieve metrics
    int data_port;
    // control port is used for health checks etc
    int control_port;
    // string defining downstreams
    char *downstream_str;
    // how often we check downstream health
    ev_tstamp downstream_health_check_interval;
    // how often we flush data
    ev_tstamp downstream_flush_interval;
    // how often we want to send ping metrics
    ev_tstamp downstream_ping_interval;
    // how many concurrent threads we run
    int threads_num;
    int socket_out_num;
    char *ping_prefix;
    int downstream_num;
    struct downstream_s *downstream;
    char health_check_response_buf[HEALTH_CHECK_RESPONSE_BUF_SIZE];
    int health_check_response_buf_length;
    struct ds_health_client_s *health_client;
    struct thread_config_s *thread_config;
    int control_socket;
};

#endif
