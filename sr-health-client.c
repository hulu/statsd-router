#include "sr-main.h"

static int setnonblock(int fd) {
    int flags = fcntl(fd, F_GETFL);
    flags |= O_NONBLOCK;
    return fcntl(fd, F_SETFL, flags);
}

static void ds_mark_down(struct ev_io *watcher) {
    int id = ((struct ev_io_id *)watcher)->id;
    if (watcher->fd > 0) {
        close(watcher->fd);
        watcher->fd = -1;
    }
    if (global.downstream[id].alive == 1) {
        global.downstream[id].active_buffer_length = 0;
        global.downstream[id].alive = 0;
        log_msg(DEBUG, "%s downstream %d is down", __func__, id);
    }
}

static void ds_health_read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    char buffer[DOWNSTREAM_HEALTH_CHECK_BUF_SIZE];
    char *expected_response = "health: up\n";
    int health_fd = watcher->fd;
    int id = ((struct ev_io_id *)watcher)->id;
    ev_io_stop(loop, watcher);
    int n = recv(health_fd, buffer, DOWNSTREAM_HEALTH_CHECK_BUF_SIZE, 0);
    if (n <= 0) {
        log_msg(WARN, "%s: recv() failed %s", __func__, strerror(errno));
        ds_mark_down(watcher);
        return;
    }
    buffer[n] = 0;
    // TODO strcmp() -> memcmp()
    if (strcmp(buffer, expected_response) != 0) {
        ds_mark_down(watcher);
        return;
    }
    if (global.downstream[id].alive == 0) {
        global.downstream[id].alive = 1;
        log_msg(DEBUG, "%s downstream %d is up", __func__, id);
    }
}

static void ds_health_send_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    char *health_check_request = "health";
    int health_check_request_length = strlen(health_check_request);
    int health_fd = watcher->fd;
    ev_io_stop(loop, watcher);
    int n = send(health_fd, health_check_request, health_check_request_length, 0);
    if (n <= 0) {
        log_msg(WARN, "%s: send() failed %s", __func__, strerror(errno));
        ds_mark_down(watcher);
        return;
    }
    ev_io_init(watcher, ds_health_read_cb, health_fd, EV_READ);
    ev_io_start(loop, watcher);
}

static void ds_health_connect_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    int health_fd = watcher->fd;
    int err;

    socklen_t len = sizeof(err);
    ev_io_stop(loop, watcher);
    getsockopt(health_fd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err) {
        ds_mark_down(watcher);
        return;
    } else {
        ev_io_init(watcher, ds_health_send_cb, health_fd, EV_WRITE);
        ev_io_start(loop, watcher);
    }
}

void ds_health_check_timer_cb(struct ev_loop *loop, struct ev_periodic *p, int revents) {
    int i;
    int health_fd;
    struct ev_io *watcher;

    for (i = 0; i < global.downstream_num; i++) {
        watcher = (struct ev_io *)(&global.downstream[i].health_watcher);
        health_fd = watcher->fd;
        if (health_fd > 0 && ev_is_active(watcher)) {
            log_msg(WARN, "%s: previous health check request was not completed for downstream %d", __func__, i);
            ev_io_stop(loop, watcher);
            ds_mark_down(watcher);
            health_fd = -1;
        }
        if (health_fd < 0) {
            health_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (health_fd == -1) {
                log_msg(WARN, "%s: socket() failed %s", __func__, strerror(errno));
                continue;
            }
            if (setnonblock(health_fd) == -1) {
                close(health_fd);
                log_msg(WARN, "%s: setnonblock() failed %s", __func__, strerror(errno));
                continue;
            }
            if (connect(health_fd, (struct sockaddr *)(&global.downstream[i].sa_in_health), sizeof(global.downstream[i].sa_in_health)) == -1 && errno == EINPROGRESS) {
                ev_io_init(watcher, ds_health_connect_cb, health_fd, EV_WRITE);
            } else {
                log_msg(WARN, "%s: connect() failed %s", __func__, strerror(errno));
                close(health_fd);
                continue;
            }
        } else {
            ev_io_init(watcher, ds_health_send_cb, health_fd, EV_WRITE);
        }
        ev_io_start(loop, watcher);
    }
}
