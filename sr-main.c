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
    int bytes_send;
    struct downstream_s *ds = (struct downstream_s *)watcher;
    int flush_buffer_idx = ds->flush_buffer_idx;

    if (EV_ERROR & revents) {
        log_msg(WARN, "%s: invalid event %s", __func__, strerror(errno));
        return;
    }

    bytes_send = sendto(watcher->fd,
        ds->buffer + flush_buffer_idx * DOWNSTREAM_BUF_SIZE,
        ds->buffer_length[flush_buffer_idx],
        0,
        (struct sockaddr *)&(ds->sa_in_data),
        sizeof(ds->sa_in_data));
    // update flush time
    ds->last_flush_time = ev_now(loop);
    ds->buffer_length[flush_buffer_idx] = 0;
    ds->flush_buffer_idx = (flush_buffer_idx + 1) % DOWNSTREAM_BUF_NUM;
    if (ds->flush_buffer_idx == ds->active_buffer_idx) {
        ev_io_stop(loop, watcher);
    }
    if (bytes_send < 0) {
        log_msg(WARN, "%s: sendto() failed %s", __func__, strerror(errno));
    }
}

// this function switches active and flush buffers, registers handler to send data when socket would be ready
void ds_schedule_flush(struct downstream_s *ds, struct ev_loop *loop) {
    struct ev_io *watcher = (struct ev_io *)ds;
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
        ev_io_init(watcher, ds_flush_cb, ds->root->socket_out, EV_WRITE);
        ev_io_start(loop, watcher);
    }
}

void push_to_downstream(struct downstream_s *ds, char *line, int length, struct ev_loop *loop) {
    // check if we new data would fit in buffer
    if (ds->active_buffer_length + length > DOWNSTREAM_BUF_SIZE) {
        // buffer is full, let's flush data
        ds_schedule_flush(ds, loop);
    }
    // let's add new data to buffer
    memcpy(ds->active_buffer + ds->active_buffer_length, line, length);
    // update buffer length
    ds->active_buffer_length += length;
}

// this function pushes data to appropriate downstream using metrics name hash
int find_downstream(char *line, unsigned long hash, int length, int downstream_num, struct downstream_s *downstream, struct ev_loop *loop) {
    // array to store downstreams for consistent hashing
    int ds_index[downstream_num];
    int i, j, k;

    log_msg(TRACE, "%s: hash = %lx, length = %d, line = %.*s", __func__, hash, length, length, line);
    // array is ordered before reshuffling
    for (i = 0; i < downstream_num; i++) {
        ds_index[i] = i;
    }
    // we have most config.downstream_num downstreams to cycle through
    for (i = downstream_num; i > 0; i--) {
        j = hash % i;
        k = ds_index[j];
        // k is downstream number for this metric, is it alive?
        if ((downstream + k)->health_client->alive) {
            log_msg(TRACE, "%s: pushing to downstream %d", __func__, k);
            push_to_downstream(downstream + k, line, length, loop);
            return 0;
        } else {
            (downstream + k)->active_buffer_length = 0;
        }
        if (j != i - 1) {
            ds_index[j] = ds_index[i - 1];
            ds_index[i - 1] = k;
        }
        // quasi random number sequence, distribution is bad without this trick
        hash = (hash * 7 + 5) / 3;
    }
    log_msg(WARN, "%s: all downstreams are dead", __func__);
    return 1;
}

// sdbm hashing (http://www.cse.yorku.ca/~oz/hash.html)
unsigned long hash(char *s, int length) {
    unsigned long h = 0;
    int i;
    for (i = 0; i < length; i++) {
        h = (h << 6) + (h << 16) - h + *(s + i);
    }
    return h;
}

// function to process single metrics line
int process_data_line(char *line, int length, int downstream_num, struct downstream_s *downstream, struct ev_loop *loop) {
    char *colon_ptr = memchr(line, ':', length);
    // if ':' wasn't found this is not valid statsd metric
    if (colon_ptr == NULL) {
        *(line + length - 1) = 0;
        log_msg(WARN, "%s: invalid metric %s", __func__, line);
        return 1;
    }
    find_downstream(line, hash(line, (colon_ptr - line)), length, downstream_num, downstream, loop);
    return 0;
}


void udp_read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    char buffer[DATA_BUF_SIZE];
    ssize_t bytes_in_buffer;
    char *buffer_ptr = buffer;
    char *delimiter_ptr = buffer;
    int line_length = 0;
    int downstream_num = ((struct ev_io_ds_s *)watcher)->downstream_num;
    struct downstream_s *downstream = ((struct ev_io_ds_s *)watcher)->downstream;

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
                process_data_line(buffer_ptr, line_length, downstream_num, downstream, loop);
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
    struct downstream_s *downstream = ((struct ev_periodic_ds_s *)p)->downstream;
    int downstream_num = ((struct ev_periodic_ds_s *)p)->downstream_num;

    for (i = 0; i < downstream_num; i++) {
        if (now - (downstream + i)->last_flush_time > ((struct ev_periodic_ds_s *)p)->interval &&
                (downstream + i)->active_buffer_length > 0) {
            ds_schedule_flush(downstream + i, loop);
        }
    }
}

void ping_cb(struct ev_loop *loop, struct ev_periodic *p, int revents) {
    int i = 0;
    int n = 0;
    int count = 0;
    char buffer[METRIC_SIZE];
    struct downstream_s *ds;
    int packets = 0;
    int traffic = 0;
    int downstream_num = ((struct ev_periodic_ds_s *)p)->downstream_num;
    struct downstream_s *downstream = ((struct ev_periodic_ds_s *)p)->downstream;
    char *alive_downstream_metric_name = ((struct ev_periodic_ds_s *)p)->string;

    for (i = 0; i < downstream_num; i++) {
        ds = downstream + i;
        if (ds->health_client->alive) {
            push_to_downstream(ds, ds->per_downstream_counter_metric, ds->per_downstream_counter_metric_length, loop);
            count++;
        }
        traffic = ds->downstream_traffic_counter;
        packets = ds->downstream_packet_counter;
        ds->downstream_traffic_counter = 0;
        ds->downstream_packet_counter = 0;
        n = sprintf(buffer, "%s:%d|c\n%s:%d|c\n",
            ds->downstream_traffic_counter_metric, traffic,
            ds->downstream_packet_counter_metric, packets);
        process_data_line(buffer, n, downstream_num, downstream, loop);
    }
    n = sprintf(buffer, "%s:%d|g\n", alive_downstream_metric_name, count);
    process_data_line(buffer, n, downstream_num, downstream, loop);
}

void *data_pipe_thread(void *args) {
    struct sockaddr_in addr;
    struct ev_loop *loop = NULL;
    struct ev_io_ds_s socket_watcher;
    struct ev_periodic_ds_s ds_flush_timer_watcher;
    struct ev_periodic_ds_s ping_timer_watcher;
    ev_tstamp ds_flush_timer_at = 0.0;
    ev_tstamp ping_timer_at = 0.0;
    int optval = 1;
    int socket_in = -1;
    sr_config_s *config = (sr_config_s *)args;
    ev_tstamp downstream_flush_interval = config->downstream_flush_interval;
    int downstream_num = config->downstream_num;
    struct downstream_s *downstream = config->downstream + config->threads_num * config->id;
    int i = 0;

    socket_in = socket(PF_INET, SOCK_DGRAM, 0);
    if (socket_in < 0 ) {
        log_msg(ERROR, "%s: socket_in socket() error %s", __func__, strerror(errno));
        return NULL;
    }
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(config->data_port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (setsockopt(socket_in, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) != 0) {
        log_msg(ERROR, "%s: setsockopt() failed %s", __func__, strerror(errno));
        return NULL;
    }

    if (bind(socket_in, (struct sockaddr*) &addr, sizeof(addr)) != 0) {
        log_msg(ERROR, "%s: bind() failed %s", __func__, strerror(errno));
        return NULL;
    }

    socket_watcher.downstream_num = downstream_num;
    socket_watcher.downstream = downstream;
    socket_watcher.socket_out = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (socket_watcher.socket_out < 0 ) {
        log_msg(ERROR, "%s: socket_out socket() error %s", __func__, strerror(errno));
        return NULL;
    }
    for (i = 0; i < downstream_num; i++) {
        (downstream + i)->root = &socket_watcher;
    }
    loop = ev_loop_new(0);
    ev_io_init((struct ev_io *)(&socket_watcher), udp_read_cb, socket_in, EV_READ);
    ev_io_start(loop, (struct ev_io *)(&socket_watcher));

    ds_flush_timer_watcher.downstream_num = downstream_num;
    ds_flush_timer_watcher.downstream = downstream;
    ds_flush_timer_watcher.interval = downstream_flush_interval;
    ev_periodic_init ((struct ev_periodic *)(&ds_flush_timer_watcher), ds_flush_timer_cb, ds_flush_timer_at, downstream_flush_interval, 0);
    ev_periodic_start (loop, (struct ev_periodic *)(&ds_flush_timer_watcher));

    ping_timer_watcher.downstream_num = downstream_num;
    ping_timer_watcher.downstream = downstream;
    ev_periodic_init((struct ev_periodic *)&ping_timer_watcher, ping_cb, ping_timer_at, config->downstream_ping_interval, 0);
    ev_periodic_start (loop, (struct ev_periodic *)&ping_timer_watcher);

    ev_loop(loop, 0);
    log_msg(ERROR, "%s: ev_loop() exited", __func__);
    return NULL;
}

int main(int argc, char *argv[]) {
    struct ev_loop *loop = ev_loop_new(0);
    struct sockaddr_in addr;
    struct ev_io_control control_socket_watcher;
    struct ev_periodic_health_client_s ds_health_check_timer_watcher;
    int i;
    int optval = 1;
    int control_socket = -1;
    ev_tstamp ds_health_check_timer_at = 0.0;
    sr_config_s config;

   if (argc != 2) {
        fprintf(stdout, "Usage: %s config.file\n", argv[0]);
        exit(1);
    }
    if (init_config(argv[1], &config) != 0) {
        log_msg(ERROR, "%s: init_config() failed", __func__);
        exit(1);
    }

    control_socket = socket(PF_INET, SOCK_STREAM, 0);
    if (control_socket < 0 ) {
        log_msg(ERROR, "%s: socket() error %s", __func__, strerror(errno));
        return(1);
    }
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(config.control_port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(control_socket, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        log_msg(ERROR, "%s: bind() failed %s", __func__, strerror(errno));
        return(1);
    }

    setsockopt(control_socket, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    if (listen(control_socket, 5) < 0) {
        log_msg(ERROR, "%s: listen() error %s", __func__, strerror(errno));
        return(1);
    }
    control_socket_watcher.health_response = config.health_check_response_buf;
    control_socket_watcher.health_response_len = &config.health_check_response_buf_length;
    ev_io_init((struct ev_io *)&control_socket_watcher, control_accept_cb, control_socket, EV_READ);
    ev_io_start(loop, (struct ev_io *)&control_socket_watcher);

    ds_health_check_timer_watcher.downstream_num = config.downstream_num;
    ds_health_check_timer_watcher.health_client = config.health_client;
    ev_periodic_init((struct ev_periodic *)&ds_health_check_timer_watcher, ds_health_check_timer_cb, ds_health_check_timer_at, config.downstream_health_check_interval, 0);
    ev_periodic_start(loop, (struct ev_periodic *)&ds_health_check_timer_watcher);

    for (i = 0; i < config.threads_num; i++) {
    }

    ev_loop(loop, 0);
    log_msg(ERROR, "%s: ev_loop() exited", __func__);
    return(0);
}
