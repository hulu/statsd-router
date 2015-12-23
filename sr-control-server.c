#include "sr-main.h"

static void control_read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);

void control_write_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    char *buffer = ((struct ev_io_control *)watcher)->buffer;
    int buffer_length = ((struct ev_io_control *)watcher)->buffer_length;
    int n;

    if (EV_ERROR & revents) {
        log_msg(WARN, "%s: invalid event %s", __func__, strerror(errno));
        return;
    }

    ev_io_stop(loop, watcher);
    if (buffer_length > 0) {
        n = send(watcher->fd, buffer, buffer_length, 0);
        if (n > 0) {
            ev_io_init(watcher, control_read_cb, watcher->fd, EV_READ);
            ev_io_start(loop, watcher);
            return;
        } else {
            log_msg(WARN, "%s: error while sending health check response", __func__);
        }
    } else {
        log_msg(WARN, "%s: nothing to send", __func__);
    }
    close(watcher->fd);
    free(watcher);
}

static void control_read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    ssize_t bytes_in_buffer;
    char buffer[CONTROL_REQUEST_BUF_SIZE];
    char *delimiter_ptr = NULL;
    int cmd_length = 0;

    if (EV_ERROR & revents) {
        log_msg(WARN, "%s: invalid event %s", __func__, strerror(errno));
        return;
    }

    ev_io_stop(loop, watcher);
    bytes_in_buffer = recv(watcher->fd, buffer, CONTROL_REQUEST_BUF_SIZE - 1, 0);
    if (bytes_in_buffer > 0) {
        while (buffer[bytes_in_buffer - 1] == '\n' || buffer[bytes_in_buffer - 1] == ' ') {
            bytes_in_buffer--;
        }
        buffer[bytes_in_buffer] = 0;
        delimiter_ptr = memchr(buffer, ' ', bytes_in_buffer);
        cmd_length = bytes_in_buffer;
        if (delimiter_ptr != NULL) {
            cmd_length = delimiter_ptr - buffer;
        }
        if (STRLEN(HEALTH_CHECK_REQUEST) == cmd_length && strncmp(HEALTH_CHECK_REQUEST, buffer, cmd_length) == 0) {
            if (delimiter_ptr != NULL) {
                global.health_check_response_buf_length = snprintf(global.health_check_response_buf, HEALTH_CHECK_RESPONSE_BUF_SIZE, "%s:%s\n", HEALTH_CHECK_REQUEST, delimiter_ptr);
            }
            ((struct ev_io_control *)watcher)->buffer = global.health_check_response_buf;
            ((struct ev_io_control *)watcher)->buffer_length = global.health_check_response_buf_length;
        }
        ev_io_init(watcher, control_write_cb, watcher->fd, EV_WRITE);
        ev_io_start(loop, watcher);
        return;
    }
    // we are here because error happened or socket is closing
    log_msg(WARN, "%s: error while reading health check request", __func__);
    close(watcher->fd);
    free(watcher);
}

void control_accept_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    int client_socket;
    struct ev_io *control_watcher;

    if (EV_ERROR & revents) {
        log_msg(WARN, "%s: invalid event %s", __func__, strerror(errno));
        return;
    }

    control_watcher = (struct ev_io*) malloc (sizeof(struct ev_io_control));
    ((struct ev_io_control *)control_watcher)->buffer_length = 0;
    if (control_watcher == NULL) {
        log_msg(ERROR, "%s: malloc() failed %s", __func__, strerror(errno));
        return;
    }
    client_socket = accept(watcher->fd, (struct sockaddr *)&client_addr, &client_addr_len);

    if (client_socket < 0) {
        log_msg(ERROR, "%s: accept() failed %s", __func__, strerror(errno));
        return;
    }

    ev_io_init(control_watcher, control_read_cb, client_socket, EV_READ);
    ev_io_start(loop, control_watcher);
}
