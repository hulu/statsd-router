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

#include "sr-main.h"

// this function flushes data to downstream
void ds_flush_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    int id = ((struct ev_io_id *)watcher)->id;
    int bytes_send;
    int flush_buffer_idx = global.downstream[id].flush_buffer_idx;

    if (EV_ERROR & revents) {
        log_msg(WARN, "%s: invalid event %s", __func__, strerror(errno));
        return;
    }

    bytes_send = sendto(watcher->fd,
        global.downstream[id].buffer + flush_buffer_idx * DOWNSTREAM_BUF_SIZE,
        global.downstream[id].buffer_length[flush_buffer_idx],
        0,
        (struct sockaddr *) (&global.downstream[id].sa_in_data),
        sizeof(global.downstream[id].sa_in_data));
    // update flush time
    global.downstream[id].last_flush_time = ev_now(loop);
    global.downstream[id].buffer_length[flush_buffer_idx] = 0;
    global.downstream[id].flush_buffer_idx = (flush_buffer_idx + 1) % DOWNSTREAM_BUF_NUM;
    if (global.downstream[id].flush_buffer_idx == global.downstream[id].active_buffer_idx) {
        ev_io_stop(loop, watcher);
    }
    if (bytes_send < 0) {
        log_msg(WARN, "%s: sendto() failed %s", __func__, strerror(errno));
    }
}

// this function switches active and flush buffers, registers handler to send data when socket would be ready
void ds_schedule_flush(struct downstream_s *ds) {
    struct ev_io *watcher = NULL;
    int new_active_buffer_idx = (ds->active_buffer_idx + 1) % DOWNSTREAM_BUF_NUM;
    // if active_buffer_idx == flush_buffer_idx this means that all previous
    // flushes are done (no filled buffers in the queue) and we need to schedule new one
    int need_to_schedule_flush = (ds->active_buffer_idx == ds->flush_buffer_idx);

    if (ds->buffer_length[new_active_buffer_idx] > 0) {
        log_msg(WARN, "%s: previous flush is not completed, loosing data.", __func__);
        ds->active_buffer_length = 0;
        return;
    }
    ds->downstream_packet_counter++;
    ds->downstream_traffic_counter += ds->active_buffer_length;
    ds->buffer_length[ds->active_buffer_idx] = ds->active_buffer_length;
    ds->active_buffer = ds->buffer + new_active_buffer_idx * DOWNSTREAM_BUF_SIZE;
    ds->active_buffer_length = 0;
    ds->active_buffer_idx = new_active_buffer_idx;
    if (need_to_schedule_flush) {
        watcher = (struct ev_io *)&(ds->flush_watcher);
        ev_io_init(watcher, ds_flush_cb, watcher->fd, EV_WRITE);
        ev_io_start(ev_default_loop(0), watcher);
    }
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

    log_msg(TRACE, "%s: hash = %lx, length = %d, line = %.*s", __func__, hash, length, length, line);
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
        if (ds->alive) {
            log_msg(TRACE, "%s: pushing to downstream %d", __func__, k);
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
    log_msg(WARN, "%s: all downstreams are dead", __func__);
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
        *(line + length - 1) = 0;
        log_msg(WARN, "%s: invalid metric %s", __func__, line);
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
        log_msg(WARN, "%s: invalid event %s", __func__, strerror(errno));
        return;
    }

    bytes_in_buffer = recv(watcher->fd, buffer, DATA_BUF_SIZE - 1, 0);

    if (bytes_in_buffer < 0) {
        log_msg(WARN, "%s: recv() failed %s", __func__, strerror(errno));
        return;
    }

    if (bytes_in_buffer > 0) {
        if (buffer[bytes_in_buffer - 1] != '\n') {
            buffer[bytes_in_buffer++] = '\n';
        }
        log_msg(TRACE, "%s: got packet %.*s", __func__, bytes_in_buffer, buffer);
        while ((delimiter_ptr = memchr(buffer_ptr, '\n', bytes_in_buffer)) != NULL) {
            delimiter_ptr++;
            line_length = delimiter_ptr - buffer_ptr;
            // minimum metrics line should look like X:1|c\n
            // so lines with length less than 6 can be ignored
            if (line_length > 5 && line_length < DOWNSTREAM_BUF_SIZE) {
                // if line has valid length let's process it
                process_data_line(buffer_ptr, line_length);
            } else {
                log_msg(WARN, "%s: invalid length %d of metric %.*s", __func__, line_length, line_length, buffer_ptr);
            }
            // this is not last metric, let's advance line start pointer
            buffer_ptr = delimiter_ptr;
            bytes_in_buffer -= line_length;
        }
    }
}

// this function cycles through downstreams and flushes them on scheduled basis
void ds_flush_timer_cb(struct ev_loop *loop, struct ev_periodic *p, int revents) {
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

void ping_cb(struct ev_loop *loop, struct ev_periodic *p, int revents) {
    int i = 0;
    int count = 0;
    char buffer[METRIC_SIZE];
    struct downstream_s *ds;
    int packets = 0;
    int traffic = 0;

    for (i = 0; i < global.downstream_num; i++) {
        ds = &global.downstream[i];
        if ((ds->health_watcher).super.fd > 0) {
            push_to_downstream(ds, ds->per_downstream_counter_metric, ds->per_downstream_counter_metric_length);
            count++;
        }
        traffic = ds->downstream_traffic_counter;
        packets = ds->downstream_packet_counter;
        ds->downstream_traffic_counter = 0;
        ds->downstream_packet_counter = 0;
        sprintf(buffer, "%s:%d|c\n%s:%d|c\n",
            ds->downstream_traffic_counter_metric, traffic,
            ds->downstream_packet_counter_metric, packets);
        process_data_line(buffer, strlen(buffer));
    }
    sprintf(buffer, "%s:%d|g\n", global.alive_downstream_metric_name, count);
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
        log_msg(ERROR, "%s: init_config() failed", __func__);
        exit(1);
    }

    for (i = 0; i < PORTS_NUM; i++) {
        switch(i) {
            case DATA_PORT_INDEX:
                type = SOCK_DGRAM;
                break;
            case CONTROL_PORT_INDEX:
                type = SOCK_STREAM;
                break;
        }
        if ((sockets[i] = socket(PF_INET, type, 0)) < 0 ) {
            log_msg(ERROR, "%s: socket() error %s", __func__, strerror(errno));
            return(1);
        }
        bzero(&addr[i], sizeof(addr[i]));
        addr[i].sin_family = AF_INET;
        addr[i].sin_port = htons(global.port[i]);
        addr[i].sin_addr.s_addr = INADDR_ANY;

        if (bind(sockets[i], (struct sockaddr*) &addr[i], sizeof(addr[i])) != 0) {
            log_msg(ERROR, "%s: bind() failed %s", __func__, strerror(errno));
            return(1);
        }

        switch(i) {
            case CONTROL_PORT_INDEX:
                optval = 1;
                setsockopt(sockets[i], SOL_SOCKET, SO_REUSEADDR, &optval, sizeof optval);
                if (listen(sockets[i], 5) < 0) {
                    log_msg(ERROR, "%s: listen() error %s", __func__, strerror(errno));
                    return(1);
                }
                ev_io_init(&socket_watcher[i], control_accept_cb, sockets[i], EV_READ);
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
    log_msg(ERROR, "%s: ev_loop() exited", __func__);
    return(0);
}


