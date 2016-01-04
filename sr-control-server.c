#include "sr-main.h"

static void control_read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents);

void control_write_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    char *response = ((struct ev_io_control *)watcher)->response;
    int response_len = ((struct ev_io_control *)watcher)->response_len;
    int n;

    if (EV_ERROR & revents) {
        log_msg(WARN, "%s: invalid event %s", __func__, strerror(errno));
        return;
    }

    ev_io_stop(loop, watcher);
    if (response_len > 0) {
        n = send(watcher->fd, response, response_len, 0);
        if (n > 0) {
            ev_io_init(watcher, control_read_cb, watcher->fd, EV_READ);
            ev_io_start(loop, watcher);
            return;
        } else {
            log_msg(WARN, "%s: error while sending control response", __func__);
        }
    } else {
        log_msg(WARN, "%s: nothing to send", __func__);
    }
    close(watcher->fd);
    free(watcher);
}

static void control_read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    ssize_t request_len;
    char request[CONTROL_REQUEST_BUF_SIZE];
    char *delimiter_ptr = NULL;
    int cmd_length = 0;
    struct ev_io_control *control_watcher = (struct ev_io_control *)watcher;

    if (EV_ERROR & revents) {
        log_msg(WARN, "%s: invalid event %s", __func__, strerror(errno));
        return;
    }

    ev_io_stop(loop, watcher);
    request_len = recv(watcher->fd, request, CONTROL_REQUEST_BUF_SIZE - 1, 0);
    if (request_len > 0) {
        while (request[request_len - 1] == '\n' || request[request_len - 1] == ' ') {
            request_len--;
        }
        request[request_len] = 0;
        delimiter_ptr = memchr(request, ' ', request_len);
        cmd_length = request_len;
        if (delimiter_ptr != NULL) {
            cmd_length = delimiter_ptr - request;
        }
        control_watcher->response_len = 0;
        if (STRLEN(HEALTH_CHECK_REQUEST) == cmd_length && strncmp(HEALTH_CHECK_REQUEST, request, cmd_length) == 0) {
            if (delimiter_ptr != NULL) {
                *control_watcher->health_response_len = snprintf(
                    control_watcher->health_response,
                    HEALTH_CHECK_RESPONSE_BUF_SIZE,
                    "%s:%s\n", HEALTH_CHECK_REQUEST, delimiter_ptr);
            }
            control_watcher->response = control_watcher->health_response;
            control_watcher->response_len = *control_watcher->health_response_len;
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
    ((struct ev_io_control *)control_watcher)->health_response_len = ((struct ev_io_control *)watcher)->health_response_len;
    ((struct ev_io_control *)control_watcher)->health_response = ((struct ev_io_control *)watcher)->health_response;
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
