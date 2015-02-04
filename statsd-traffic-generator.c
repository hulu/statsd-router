/**
 * statsd-traffic-generator: generates traffic for testing of statsd-cluster
 * (https://github.com/etsy/statsd/).
 *
 * Author: Kirill Timofeev <kvt@hulu.com>
 *	
 * Enjoy :-)!	
 *	
**/

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

// Size of buffer for outgoing packets. Should be below MTU.
// TODO Probably should be configured via configuration file?
#define DOWNSTREAM_BUF_SIZE 1450
// Size of other temporary buffers
#define LOG_BUF_SIZE 2048

// structure that holds downstream data
struct downstream_s {
    // sockaddr for data
    struct sockaddr_in sa_in_data;
    // id extended ev_io structure used for sending data to downstream
    struct ev_io flush_watcher;
    // last time data was flushed to downstream
    ev_tstamp last_flush_time;
};

// globally accessed structure with commonly used data
struct global_s {
    struct downstream_s downstream;
    // how often we flush data
    ev_tstamp downstream_flush_interval;
    // how noisy is our log
    int log_level;
};

struct global_s global;

// numeric values for log levels
enum log_level_e {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR
};

// and function to convert numeric values into strings
char *log_level_name(enum log_level_e level) {
    static char *name[] = { "TRACE", "DEBUG", "INFO", "WARN", "ERROR"};
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
    vsnprintf(buffer + l, LOG_BUF_SIZE - l, format, args);
    va_end(args);
    fprintf(stdout, "%s\n", buffer);
    fflush(stdout);
}

void ds_flush_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    int bytes_send;
    static int counter = 0;
    char buffer[DOWNSTREAM_BUF_SIZE];

    if (EV_ERROR & revents) {
        log_msg(ERROR, "%s: invalid event %s", __func__, strerror(errno));
        return;
    }

    ev_io_stop(loop, watcher);
    counter = (counter + 1) % 100;
    sprintf(buffer, "test.counter%d:1|c\n", counter);
    bytes_send = sendto(watcher->fd,
        buffer,
        strlen(buffer),
        0,
        (struct sockaddr *) (&global.downstream.sa_in_data),
        sizeof(global.downstream.sa_in_data));
    if (bytes_send < 0) {
        log_msg(ERROR, "%s: sendto() failed %s", __func__, strerror(errno));
    }
}

void ds_flush_timer_cb(struct ev_loop *loop, struct ev_periodic *p, int revents) {
    struct ev_io *watcher = NULL;

    watcher = &(global.downstream.flush_watcher);
    if (ev_is_active(watcher)) {
        return;
    }
    ev_io_init(watcher, ds_flush_cb, watcher->fd, EV_WRITE);
    ev_io_start(ev_default_loop(0), watcher);
}

void init_sockaddr_in(struct sockaddr_in *sa_in, char *host, char *port) {
    struct hostent *he = gethostbyname(host);

    if (he == NULL || he->h_addr_list == NULL || (he->h_addr_list)[0] == NULL ) {
        log_msg(ERROR, "%s: gethostbyname() failed %s", __func__, strerror(errno));
        return;
    }
    bzero(sa_in, sizeof(*sa_in));
    sa_in->sin_family = AF_INET;
    sa_in->sin_port = htons(atoi(port));
    memcpy(&(sa_in->sin_addr), he->h_addr_list[0], he->h_length);
}

// function to init downstream from config file line
int init_downstream(char *hosts) {
    char *host = hosts;
    char *data_port = NULL;

    // argument line has the following format: host:data_port
    // now let's initialize downstreams
    global.downstream.last_flush_time = ev_time();
    global.downstream.flush_watcher.fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);;
    if (global.downstream.flush_watcher.fd < 0) {
        log_msg(ERROR, "%s: socket() failed %s", __func__, strerror(errno));
        return 1;
    }
    data_port = strchr(host, ':');
    if (data_port == NULL) {
        log_msg(ERROR, "%s: no data port for %s", __func__, host);
        return 1;
    }
    *data_port++ = 0;
    init_sockaddr_in(&global.downstream.sa_in_data, host, data_port);
    return 0;
}

// function to parse single line from config file
int process_config_line(char *line) {
    // valid line should contain '=' symbol
    char *value_ptr = strchr(line, '=');
    if (value_ptr == NULL) {
        log_msg(ERROR, "%s: bad line in config \"%s\"", __func__, line);
        return 1;
    }
    *value_ptr++ = 0;
    if (strcmp("downstream_flush_interval", line) == 0) {
        global.downstream_flush_interval = atof(value_ptr);
    } else if (strcmp("log_level", line) == 0) {
        global.log_level = atoi(value_ptr);
    } else if (strcmp("downstream", line) == 0) {
        return init_downstream(value_ptr);
    } else {
        log_msg(ERROR, "%s: unknown parameter \"%s\"", __func__, line);
        return 1;
    }
    return 0;
}

// this function is called if SIGHUP is received
void on_sighup(int sig) {
    log_msg(INFO, "%s: sighup received", __func__);
}

void on_sigint(int sig) {
    log_msg(INFO, "%s: sigint received", __func__);
    exit(0);
}

// this function loads config file and initializes config fields
int init_config(char *filename) {
    size_t n = 0;
    int l = 0;
    int failures = 0;
    char *buffer;

    global.log_level = 0;
    FILE *config_file = fopen(filename, "rt");
    if (config_file == NULL) {
        log_msg(ERROR, "%s: fopen() failed %s", __func__, strerror(errno));
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
        log_msg(ERROR, "%s: failed to load config file", __func__);
        return 1;
    }
    if (signal(SIGHUP, on_sighup) == SIG_ERR) {
        log_msg(ERROR, "%s: signal() failed", __func__);
        return 1;
    }
    if (signal(SIGINT, on_sigint) == SIG_ERR) {
        log_msg(ERROR, "%s: signal() failed", __func__);
        return 1;
    }
    return 0;
}

// program entry point
int main(int argc, char *argv[]) {
    struct ev_loop *loop = ev_default_loop(0);
    struct ev_periodic ds_flush_timer_watcher;
    ev_tstamp ds_flush_timer_at = 0.0;

   if (argc != 2) {
        fprintf(stdout, "Usage: %s config.file\n", argv[0]);
        exit(1);
    }
    if (init_config(argv[1]) != 0) {
        log_msg(ERROR, "%s: init_config() failed", __func__);
        exit(1);
    }

    ev_periodic_init (&ds_flush_timer_watcher, ds_flush_timer_cb, ds_flush_timer_at, global.downstream_flush_interval, 0);
    ev_periodic_start (loop, &ds_flush_timer_watcher);

    ev_loop(loop, 0);
    log_msg(ERROR, "%s: ev_loop() exited", __func__);
    return(0);
}
