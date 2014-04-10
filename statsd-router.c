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

#define HEALTHY_DOWNSTREAMS "healthy_downstreams"
#define PER_DOWNSTREAM_COUNTER_METRIC_SUFFIX "connections:1|c"

// Size of buffer for outgoing packets. Should be below MTU.
// TODO Probably should be configured via configuration file?
#define DOWNSTREAM_BUF_SIZE 1450
// Size of other temporary buffers
#define DATA_BUF_SIZE 4096
#define DOWNSTREAM_HEALTH_CHECK_BUF_SIZE 32
#define HEALTH_CHECK_BUF_SIZE 512
#define LOG_BUF_SIZE 256
#define METRIC_SIZE 256

// statsd-router ports are stored in array and accessed using indexies below
#define DATA_PORT_INDEX 0
#define HEALTH_PORT_INDEX 1
// number of ports used by statsd-router
#define PORTS_NUM 2

// extended ev structure with id field
// used to check downstream health
struct ev_io_id {
    struct ev_io super;
    int id;
};

// extended ev structure with buffer and buffer length
// used by health check clients
struct health_check_ev_io {
    struct ev_io ev_io_orig;
    char buffer[HEALTH_CHECK_BUF_SIZE];
    int buffer_length;
};

// structure that holds downstream data
struct downstream_s {
    // buffer where data is added
    char *active_buffer;
    int active_buffer_length;
    // buffer ready for flush
    char *flush_buffer;
    int flush_buffer_length;
    // memory for both active and flush buffer
    char buffer[DOWNSTREAM_BUF_SIZE * 2];
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
    // this would allow us to detect metris loss and locate
    // statsd-router to statsd connection with data loss
    char per_downstream_counter_metric[METRIC_SIZE];
    int per_downstream_counter_metric_length;
    // flag to prevent scheduling flush if previous is not completed
    int flush_in_progress;
};

// globally accessed structure with commonly used data
struct global_s {
    // statsd-router ports, accessed via DATA_PORT_INDEX and HEALTH_PORT_INDEX
    int port[PORTS_NUM];
    // how many downstreams we have
    int downstream_num;
    // array of downstreams
    struct downstream_s *downstream;
    // how often we check downstream health
    ev_tstamp downstream_health_check_interval;
    // how often we flush data
    ev_tstamp downstream_flush_interval;
    // log file related fields
    char *log_file_name;
    FILE *log_file;
    int log_level;
    // how often we want to send ping metrics
    ev_tstamp downstream_ping_interval;
    char alive_downstream_metric_name[METRIC_SIZE];
};

struct global_s global;

// we have this forward declaration here since this callback is using other callback
// one of them should be declared forward
void health_read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);

// numeric values for log levels
enum log_level_e {
    INFO,
    DEBUG,
    WARN,
    ERROR
};

// and function to convert numeric values into strings
char *log_level_name(enum log_level_e level) {
    static char *name[] = { "INFO", "DEBUG", "WARN", "ERROR"};
    return name[level];
}

// function to log message
void log_msg(int level, char *format, ...) {
    va_list args;
    time_t t;
    struct tm *tinfo;
    char buffer[LOG_BUF_SIZE];
    int l = 0;

    if (level < global.log_level) {
        return;
    }
    va_start(args, format);
    time(&t);
    tinfo = localtime(&t);
    l = strftime(buffer, LOG_BUF_SIZE, "%Y-%m-%d %H:%M:%S", tinfo);
    l += sprintf(buffer + l, " %s ", log_level_name(level));
    vsprintf(buffer + l, format, args);
    va_end(args);
    fprintf(global.log_file, "%s\n", buffer);
}

// this function flushes data to downstream
void ds_flush_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    int id = ((struct ev_io_id *)watcher)->id;
    int bytes_send;

    if (EV_ERROR & revents) {
        log_msg(ERROR, "udp_read_cb: invalid event %s", strerror(errno));
        return;
    }

    bytes_send = sendto(watcher->fd,
        global.downstream[id].flush_buffer,
        global.downstream[id].flush_buffer_length,
        0,
        (struct sockaddr *) (&global.downstream[id].sa_in_data),
        sizeof(global.downstream[id].sa_in_data));
    // update flush time
    global.downstream[id].last_flush_time = ev_now(loop);
    global.downstream[id].flush_in_progress = 0;
    ev_io_stop(loop, watcher);
    if (bytes_send < 0) {
        log_msg(ERROR, "ds_flush_cb: sendto() failed %s", strerror(errno));
    }
}

// this function switches active and flush buffers, registers handler to send data when socket would be ready
void ds_schedule_flush(struct downstream_s *ds) {
    char *t = NULL;
    struct ev_io *watcher = NULL;

    if (ds->flush_in_progress == 1) {
        log_msg(ERROR, "Previous flush is not completed, loosing data.");
        ds->active_buffer_length = 0;
        return;
    }
    ds->flush_in_progress = 1;
    t = ds->active_buffer;
    watcher = (struct ev_io *)&(ds->flush_watcher);
    ds->active_buffer = ds->flush_buffer;
    ds->flush_buffer = t;
    ds->flush_buffer_length = ds->active_buffer_length;
    ds->active_buffer_length = 0;
    ev_io_init(watcher, ds_flush_cb, watcher->fd, EV_WRITE);
    ev_io_start(ev_default_loop(0), watcher);
}

void push_to_downstream(struct downstream_s *ds, char *line, int length) {
    // check if we new data would fit in buffer
    if (ds->active_buffer_length + length > DOWNSTREAM_BUF_SIZE) {
        // buffer is full, let's flush data
        ds_schedule_flush(ds);
    }
    // let's add new data to buffer
    memcpy(ds->active_buffer + ds->active_buffer_length, line, length);
    // update buffer length
    ds->active_buffer_length += length;
}

// this function pushes data to appropriate downstream using metrics name hash
int find_downstream(char *line, unsigned long hash, int length) {
    // array to store downstreams for consistent hashing
    int downstream[global.downstream_num];
    int i, j, k;
    struct downstream_s *ds;

    // array is ordered before reshuffling
    for (i = 0; i < global.downstream_num; i++) {
        downstream[i] = i;
    }
    // we have most config.downstream_num downstreams to cycle through
    for (i = global.downstream_num; i > 0; i--) {
        j = hash % i;
        k = downstream[j];
        // k is downstream number for this metric, is it alive?
        ds = &(global.downstream[k]);
        if ((ds->health_watcher).super.fd > 0) {
            push_to_downstream(ds, line, length);
            return 0;
        }
        if (j != i - 1) {
            downstream[j] = downstream[i - 1];
            downstream[i - 1] = k;
        }
        // quasi random number sequence, distribution is bad without this trick
        hash = (hash * 7 + 5) / 3;
    }
    log_msg(ERROR, "all downstreams are dead");
    return 1;
}

// simple hash code calculation borrowed from java
unsigned long hash(char *s, int length) {
    unsigned long h = 0;
    int i;
    for (i = 0; i < length; i++) {
        h = h * 31 + *(s + i);
    }
    return h;
}

// function to process single metrics line
int process_data_line(char *line, int length) {
    char *colon_ptr = memchr(line, ':', length);
    // if ':' wasn't found this is not valid statsd metric
    if (colon_ptr == NULL) {
        log_msg(ERROR, "process_line: invalid metric %s", line);
        return 1;
    }
    find_downstream(line, hash(line, (colon_ptr - line)), length);
    return 0;
}


void udp_read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    char buffer[DATA_BUF_SIZE];
    ssize_t bytes_in_buffer;
    char *buffer_ptr = buffer;
    char *delimiter_ptr = buffer;
    int line_length = 0;

    if (EV_ERROR & revents) {
        log_msg(ERROR, "udp_read_cb: invalid event %s", strerror(errno));
        return;
    }

    bytes_in_buffer = recv(watcher->fd, buffer, DATA_BUF_SIZE - 1, 0);

    if (bytes_in_buffer < 0) {
        log_msg(ERROR, "udp_read_cb: read() failed %s", strerror(errno));
        return;
    }

    if (bytes_in_buffer > 0) {
        buffer[bytes_in_buffer++] = '\n';
        while ((delimiter_ptr = memchr(buffer_ptr, '\n', bytes_in_buffer)) != NULL) {
            delimiter_ptr++;
            line_length = delimiter_ptr - buffer_ptr;
            // minimum metrics line should look like X:1|c
            // so lines with length less than 5 can be ignored
            if (line_length > 5) {
                // if line is not empty let's process it
                process_data_line(buffer_ptr, line_length);
            }
            // this is not last metric, let's advance line start pointer
            buffer_ptr = delimiter_ptr;
            bytes_in_buffer -= line_length;
        }
    }
}

// this fuction cycles through downstreams and flushes them on scheduled basis
void ds_flush_timer_cb(struct ev_loop *loop, struct ev_io *w, int revents) {
    int i;
    ev_tstamp now = ev_now(loop);
    struct downstream_s *ds;

    for (i = 0; i < global.downstream_num; i++) {
        ds = &(global.downstream[i]);
        if (now - ds->last_flush_time > global.downstream_flush_interval &&
                ds->active_buffer_length > 0) {
            ds_schedule_flush(ds);
        }
    }
}

void init_sockaddr_in(struct sockaddr_in *sa_in, char *host, char *port) {
    struct hostent *he = gethostbyname(host);

    if (he == NULL || he->h_addr_list == NULL || (he->h_addr_list)[0] == NULL ) {
        log_msg(ERROR, "init_sockaddr_in: gethostbyname() failed %s", strerror(errno));
        return;
    }
    bzero(sa_in, sizeof(*sa_in));
    sa_in->sin_family = AF_INET;
    sa_in->sin_port = htons(atoi(port));
    memcpy(&(sa_in->sin_addr), he->h_addr_list[0], he->h_length);
}

// function to init downstreams from config file line
int init_downstream(char *hosts) {
    int i = 0;
    int j = 0;
    char *host = hosts;
    char *next_host = NULL;
    char *data_port = NULL;
    char *health_port = NULL;
    char per_connection_prefix[METRIC_SIZE];
    char per_downstream_prefix[METRIC_SIZE];

    // argument line has the following format: host1:data_port1:health_port1,host2:data_port2:healt_port2,...
    // number of downstreams is equal to number of commas + 1
    global.downstream_num = 1;
    while (hosts[i] != 0) {
        if (hosts[i++] == ',') {
            global.downstream_num++;
        }
    }
    global.downstream = (struct downstream_s *)malloc(sizeof(struct downstream_s) * global.downstream_num);
    if (global.downstream == NULL) {
        log_msg(ERROR, "init_downstream: malloc() failed %s", strerror(errno));
        return 1;
    }
    strcpy(per_connection_prefix, global.alive_downstream_metric_name);
    for (i = strlen(per_connection_prefix); i > 0; i--) {
        if (per_connection_prefix[i] == '.') {
            per_connection_prefix[i] = 0;
            break;
        }
    }
    strcpy(per_downstream_prefix, per_connection_prefix);
    for (i = strlen(per_downstream_prefix); i > 0; i--) {
        if (per_downstream_prefix[i] == '.') {
            per_downstream_prefix[i] = 0;
            break;
        }
    }
    // now let's initialize downstreams
    for (i = 0; i < global.downstream_num; i++) {
        global.downstream[i].last_flush_time = ev_time();
        global.downstream[i].flush_in_progress = 0;
        global.downstream[i].active_buffer = global.downstream[i].buffer;
        global.downstream[i].flush_buffer = global.downstream[i].buffer + DOWNSTREAM_BUF_SIZE;
        global.downstream[i].active_buffer_length = 0;
        global.downstream[i].flush_buffer_length = 0;
        global.downstream[i].health_watcher.super.fd = -1;
        global.downstream[i].health_watcher.id = i;
        global.downstream[i].flush_watcher.super.fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);;
        global.downstream[i].flush_watcher.id = i;
        if (global.downstream[i].flush_watcher.super.fd < 0) {
            log_msg(ERROR, "init_downstream: socket() failed %s", strerror(errno));
            return 1;
        }
        if (host == NULL) {
            log_msg(ERROR, "init_downstream: null hostname at iteration %d", i);
            return 1;
        }
        next_host = strchr(host, ',');
        if (next_host != NULL) {
            *next_host++ = 0;
        }
        data_port = strchr(host, ':');
        if (data_port == NULL) {
            log_msg(ERROR, "init_downstream: no data port for %s", host);
            return 1;
        }
        *data_port++ = 0;
        health_port = strchr(data_port, ':');
        if (health_port == NULL) {
            log_msg(ERROR, "init_downstream: no health_port for %s", host);
            return 1;
        }
        *health_port++ = 0;
        init_sockaddr_in(&global.downstream[i].sa_in_data, host, data_port);
        init_sockaddr_in(&global.downstream[i].sa_in_health, host, health_port);
        for (j = 0; *(host + j) != 0; j++) {
            if (*(host + j) == '.') {
                *(host + j) = '_';
            }
        }
        sprintf(global.downstream[i].per_downstream_counter_metric, "%s-%s-%s.%s\n%s.%s-%s.%s\n",
            per_connection_prefix, host, data_port, PER_DOWNSTREAM_COUNTER_METRIC_SUFFIX,
            per_downstream_prefix, host, data_port, PER_DOWNSTREAM_COUNTER_METRIC_SUFFIX);
        global.downstream[i].per_downstream_counter_metric_length = strlen(global.downstream[i].per_downstream_counter_metric);
        host = next_host;
    }
    return 0;
}

// function to parse single line from config file
int process_config_line(char *line) {
    char buffer[METRIC_SIZE];
    int l = 0;
    int n;
    // valid line should contain '=' symbol
    char *value_ptr = strchr(line, '=');
    if (value_ptr == NULL) {
        log_msg(ERROR, "process_config_line: bad line in config \"%s\"", line);
        return 1;
    }
    *value_ptr++ = 0;
    if (strcmp("data_port", line) == 0) {
        global.port[DATA_PORT_INDEX] = atoi(value_ptr);
    } else if (strcmp("health_port", line) == 0) {
        global.port[HEALTH_PORT_INDEX] = atoi(value_ptr);
    } else if (strcmp("downstream_flush_interval", line) == 0) {
        global.downstream_flush_interval = atof(value_ptr);
    } else if (strcmp("downstream_health_check_interval", line) == 0) {
        global.downstream_health_check_interval = atof(value_ptr);
    } else if (strcmp("downstream_ping_interval", line) == 0) {
        global.downstream_ping_interval = atof(value_ptr);
    } else if (strcmp("log_level", line) == 0) {
        global.log_level = atoi(value_ptr);
    } else if (strcmp("ping_prefix", line) == 0) {
        n = gethostname(buffer, METRIC_SIZE);
        if (n < 0) {
            log_msg(ERROR, "process_config_line: gethostname() failed");
            return 1;
        }
        sprintf(global.alive_downstream_metric_name, "%s.%s-%d.%s", value_ptr, buffer, global.port[DATA_PORT_INDEX], HEALTHY_DOWNSTREAMS);
    } else if (strcmp("log_file_name", line) == 0) {
        l = strlen(value_ptr) + 1;
        global.log_file_name = (char *)malloc(l);
        if (global.log_file_name == NULL) {
            log_msg(ERROR, "process_config_line: malloc() failed %s", strerror(errno));
            return 1;
        }
        strcpy(global.log_file_name, value_ptr);
        global.log_file = NULL;
        *(global.log_file_name + l) = 0;
    } else if (strcmp("downstream", line) == 0) {
        return init_downstream(value_ptr);
    } else {
        log_msg(ERROR, "process_config_line: unknown parameter \"%s\"", line);
        return 1;
    }
    return 0;
}

int reopen_log() {
    if (global.log_file_name != NULL) {
        if (global.log_file != NULL) {
            fclose(global.log_file);
        }
        global.log_file = fopen(global.log_file_name, "a");
        if (global.log_file == NULL) {
            log_msg(ERROR, "reopen_log: fopen() failed %s", strerror(errno));
            return 1;
        }
    }
    return 0;
}

// this function is called if SIGHUP is recieved
void on_sighup(int sig) {
    reopen_log();
}

void on_sigint(int sig) {
    fflush(global.log_file);
    exit(0);
}

// this function loads config file and initializes config fields
int init_config(char *filename) {
    size_t n = 0;
    int l = 0;
    int failures = 0;
    char *buffer;

    global.log_file_name = NULL;
    global.log_file = stderr;
    global.log_level = 0;
    FILE *config_file = fopen(filename, "rt");
    if (config_file == NULL) {
        log_msg(ERROR, "init_config: fopen() failed %s", strerror(errno));
        return 1;
    }
    // config file can contain very long lines e.g. to specify downstreams
    // using getline() here since it automatically adjusts buffer
    while ((l = getline(&buffer, &n, config_file)) > 0) {
        if (buffer[l - 1] == '\n') {
            buffer[l - 1] = 0;
        }
        if (buffer[0] != '\n' && buffer[0] != '#') {
            failures += process_config_line(buffer);
        }
    }
    // buffer is reused by getline() so we need to free it only once
    free(buffer);
    fclose(config_file);
    if (failures > 0) {
        log_msg(ERROR, "init_config: failed to load config file");
        return 1;
    }
    if (reopen_log() != 0) {
        return 1;
    }
    if (signal(SIGHUP, on_sighup) == SIG_ERR) {
        log_msg(ERROR, "init_config: signal() failed");
        return 1;
    }
    if (signal(SIGINT, on_sigint) == SIG_ERR) {
        log_msg(ERROR, "init_config: signal() failed");
        return 1;
    }
    return 0;
}

// self health check related functions

void health_write_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    char *buffer = ((struct health_check_ev_io *)watcher)->buffer;
    int buffer_length = ((struct health_check_ev_io *)watcher)->buffer_length;
    int n;

    if (EV_ERROR & revents) {
        log_msg(ERROR, "health_write_cb: invalid event %s", strerror(errno));
        return;
    }

    n = send(watcher->fd, buffer, buffer_length, 0);
    ev_io_stop(loop, watcher);
    if (n > 0) {
        ((struct health_check_ev_io *)watcher)->buffer_length = 0;
        ev_io_init(watcher, health_read_cb, watcher->fd, EV_READ);
        ev_io_start(loop, watcher);
        return;
    }
    close(watcher->fd);
    free(watcher);
}

void health_read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    char *buffer = ((struct health_check_ev_io *)watcher)->buffer;
    int buffer_length = ((struct health_check_ev_io *)watcher)->buffer_length;
    ssize_t read;

    if (EV_ERROR & revents) {
        log_msg(ERROR, "health_read_cb: invalid event %s", strerror(errno));
        return;
    }

    read = recv(watcher->fd, (buffer + buffer_length), (HEALTH_CHECK_BUF_SIZE - buffer_length), 0);
    ev_io_stop(loop, watcher);
    if (read > 0) {
        buffer_length += read;
        buffer[buffer_length] = 0;
        ((struct health_check_ev_io *)watcher)->buffer_length = buffer_length;
        ev_io_init(watcher, health_write_cb, watcher->fd, EV_WRITE);
        ev_io_start(loop, watcher);
        return;
    }
    // we are here because error happened or socket is closing
    close(watcher->fd);
    free(watcher);
}

void health_accept_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    int client_socket;
    struct ev_io *health_read_watcher;

    if (EV_ERROR & revents) {
        log_msg(ERROR, "health_accept_cb: invalid event %s", strerror(errno));
        return;
    }

    health_read_watcher = (struct ev_io*) malloc (sizeof(struct health_check_ev_io));
    if (health_read_watcher == NULL) {
        log_msg(ERROR, "health_accept_cb: malloc() failed %s", strerror(errno));
        return;
    }
    ((struct health_check_ev_io *)health_read_watcher)->buffer_length = 0;
    client_socket = accept(watcher->fd, (struct sockaddr *)&client_addr, &client_addr_len);

    if (client_socket < 0) {
        log_msg(ERROR, "health_accept_cb: accept() failed %s", strerror(errno));
        return;
    }

    ev_io_init(health_read_watcher, health_read_cb, client_socket, EV_READ);
    ev_io_start(loop, health_read_watcher);
}

// downstream health check functions

int setnonblock(int fd) {
    int flags = fcntl(fd, F_GETFL);
    flags |= O_NONBLOCK;
    return fcntl(fd, F_SETFL, flags);
}

void ds_mark_down(struct ev_io *watcher) {
    int id = ((struct ev_io_id *)watcher)->id;
    if (watcher->fd > 0) {
        close(watcher->fd);
        watcher->fd = -1;
    }
    global.downstream[id].active_buffer_length = 0;
}

void ds_health_read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    char buffer[DOWNSTREAM_HEALTH_CHECK_BUF_SIZE];
    char *expected_response = "health: up\n";
    int health_fd = watcher->fd;
    int n = recv(health_fd, buffer, DOWNSTREAM_HEALTH_CHECK_BUF_SIZE, 0);
    ev_io_stop(loop, watcher);
    if (n <= 0) {
        log_msg(ERROR, "ds_health_read_cb: read() failed %s", strerror(errno));
        ds_mark_down(watcher);
        return;
    }
    buffer[n] = 0;
    if (strcmp(buffer, expected_response) != 0) {
        ds_mark_down(watcher);
    }
}

void ds_health_send_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    char *health_check_request = "health";
    int health_check_request_length = strlen(health_check_request);
    int health_fd = watcher->fd;
    int n = send(health_fd, health_check_request, health_check_request_length, 0);
    ev_io_stop(loop, watcher);
    if (n <= 0) {
        log_msg(ERROR, "ds_health_send_cb: send() failed %s", strerror(errno));
        ds_mark_down(watcher);
        return;
    }
    ev_io_init(watcher, ds_health_read_cb, health_fd, EV_READ);
    ev_io_start(loop, watcher);
}

void ds_health_connect_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    int health_fd = watcher->fd;
    int err;

    socklen_t len = sizeof(err);
    getsockopt(health_fd, SOL_SOCKET, SO_ERROR, &err, &len);
    ev_io_stop(loop, watcher);
    if (err) {
        ds_mark_down(watcher);
        return;
    } else {
        ev_io_init(watcher, ds_health_send_cb, health_fd, EV_WRITE);
        ev_io_start(loop, watcher);
    }
}

void ds_health_check_timer_cb(struct ev_loop *loop, struct ev_io *w, int revents) {
    int i;
    int health_fd;
    struct ev_io *watcher;

    for (i = 0; i < global.downstream_num; i++) {
        watcher = (struct ev_io *)(&global.downstream[i].health_watcher);
        health_fd = watcher->fd;
        if (health_fd < 0) {
            health_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (health_fd > 0 && setnonblock(health_fd) == -1) {
                log_msg(ERROR, "ds_health_check_timer_cb: setnonblock() failed %s", strerror(errno));
                ds_mark_down(watcher);
                continue;
            }
            connect(health_fd, (struct sockaddr *)(&global.downstream[i].sa_in_health), sizeof(global.downstream[i].sa_in_health));
            ev_io_init(watcher, ds_health_connect_cb, health_fd, EV_WRITE);
        } else {
            ev_io_init(watcher, ds_health_send_cb, health_fd, EV_WRITE);
        }
        ev_io_start(loop, watcher);
    }
}

void ping_cb(struct ev_loop *loop, struct ev_io *w, int revents) {
    int i = 0;
    int count = 0;
    char buffer[METRIC_SIZE];
    struct downstream_s *ds;

    for (i = 0; i < global.downstream_num; i++) {
        ds = &global.downstream[i];
        if ((ds->health_watcher).super.fd > 0) {
            push_to_downstream(ds, ds->per_downstream_counter_metric, ds->per_downstream_counter_metric_length);
            count++;
        }
    }
    sprintf(buffer, "%s:%d|c\n", global.alive_downstream_metric_name, count);
    process_data_line(buffer, strlen(buffer));
}

// program entry point
int main(int argc, char *argv[]) {
    struct ev_loop *loop = ev_default_loop(0);
    int sockets[PORTS_NUM];
    struct sockaddr_in addr[PORTS_NUM];
    struct ev_io socket_watcher[PORTS_NUM];
    struct ev_periodic ds_health_check_timer_watcher;
    struct ev_periodic ds_flush_timer_watcher;
    struct ev_periodic ping_timer_watcher;
    int i;
    int type;
    int optval;
    ev_tstamp ds_health_check_timer_at = 0.0;
    ev_tstamp ds_flush_timer_at = 0.0;
    ev_tstamp ping_timer_at = 0.0;

   if (argc != 2) {
        fprintf(stdout, "Usage: %s config.file\n", argv[0]);
        exit(1);
    }
    if (init_config(argv[1]) != 0) {
        log_msg(ERROR, "init_config() failed");
        exit(1);
    }

    for (i = 0; i < PORTS_NUM; i++) {
        switch(i) {
            case DATA_PORT_INDEX:
                type = SOCK_DGRAM;
                break;
            case HEALTH_PORT_INDEX:
                type = SOCK_STREAM;
                break;
        }
        if ((sockets[i] = socket(PF_INET, type, 0)) < 0 ) {
            log_msg(ERROR, "main: socket() error %s", strerror(errno));
            return(1);
        }
        bzero(&addr[i], sizeof(addr[i]));
        addr[i].sin_family = AF_INET;
        addr[i].sin_port = htons(global.port[i]);
        addr[i].sin_addr.s_addr = INADDR_ANY;

        if (bind(sockets[i], (struct sockaddr*) &addr[i], sizeof(addr[i])) != 0) {
            log_msg(ERROR, "main: bind() failed %s", strerror(errno));
            return(1);
        }

        switch(i) {
            case HEALTH_PORT_INDEX:
                optval = 1;
                setsockopt(sockets[i], SOL_SOCKET, SO_REUSEADDR, &optval, sizeof optval);
                if (listen(sockets[i], 5) < 0) {
                    log_msg(ERROR, "main: listen() error %s", strerror(errno));
                    return(1);
                }
                ev_io_init(&socket_watcher[i], health_accept_cb, sockets[i], EV_READ);
                break;
            case DATA_PORT_INDEX:
                ev_io_init(&socket_watcher[i], udp_read_cb, sockets[i], EV_READ);
                break;
        }
        ev_io_start(loop, &socket_watcher[i]);
    }

    ev_periodic_init (&ds_health_check_timer_watcher, ds_health_check_timer_cb, ds_health_check_timer_at, global.downstream_health_check_interval, 0);
    ev_periodic_start (loop, &ds_health_check_timer_watcher);
    ev_periodic_init (&ds_flush_timer_watcher, ds_flush_timer_cb, ds_flush_timer_at, global.downstream_flush_interval, 0);
    ev_periodic_start (loop, &ds_flush_timer_watcher);
    ev_periodic_init(&ping_timer_watcher, ping_cb, ping_timer_at, global.downstream_ping_interval, 0);
    ev_periodic_start (loop, &ping_timer_watcher);

    ev_loop(loop, 0);
    log_msg(ERROR, "main: ev_loop() exited");
    return(0);
}


