#include "sr-main.h"

static int setnonblock(int fd) {
    int flags = fcntl(fd, F_GETFL);
    flags |= O_NONBLOCK;
    return fcntl(fd, F_SETFL, flags);
}

static void ds_mark_down(struct ev_io *watcher) {
    struct ds_health_client_s *health_client = (struct ds_health_client_s *)watcher;
    if (watcher->fd > 0) {
        close(watcher->fd);
        watcher->fd = -1;
    }
    if (health_client->alive == 1) {
        health_client->alive = 0;
        log_msg(DEBUG, "%s downstream %d is down", __func__, health_client->id);
    }
}

static void ds_health_read_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    struct ds_health_client_s *health_client = (struct ds_health_client_s *)watcher;
    char buffer[DOWNSTREAM_HEALTH_CHECK_BUF_SIZE];
    int health_fd = watcher->fd;
    ev_io_stop(loop, watcher);
    int n = recv(health_fd, buffer, DOWNSTREAM_HEALTH_CHECK_BUF_SIZE, 0);
    if (n <= 0) {
        log_msg(WARN, "%s: recv() failed %s", __func__, strerror(errno));
        ds_mark_down(watcher);
        return;
    }
    buffer[n] = 0;
    if (memcmp(buffer, HEALTH_CHECK_UP_RESPONSE, STRLEN(HEALTH_CHECK_UP_RESPONSE)) != 0) {
        ds_mark_down(watcher);
        return;
    }
    if (health_client->alive == 0) {
        health_client->alive = 1;
        log_msg(DEBUG, "%s downstream %d is up", __func__, health_client->id);
    }
}

static void ds_health_send_cb(struct ev_loop *loop, struct ev_io *watcher, int revents) {
    int health_fd = watcher->fd;
    ev_io_stop(loop, watcher);
    int n = send(health_fd, HEALTH_CHECK_REQUEST, STRLEN(HEALTH_CHECK_REQUEST), 0);
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

// TODO this code behaves oddly if the remote health server crashes after reading request

void ds_health_check_timer_cb(struct ev_loop *loop, struct ev_periodic *p, int revents) {
    int i;
    int health_fd;
    struct ev_io *watcher;
    struct ev_periodic_health_client_s *ev_periodic_hc = (struct ev_periodic_health_client_s *)p;
    int downstream_num = ev_periodic_hc->downstream_num;
    struct ds_health_client_s *health_client = ev_periodic_hc->health_client;
    int n = 0;

    for (i = 0; i < downstream_num; i++) {
        watcher = (struct ev_io *)(health_client + i);
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
            n = connect(health_fd, (struct sockaddr *)&((health_client + i)->sa_in), sizeof((health_client + i)->sa_in));
            if (n == -1 && errno == EINPROGRESS) {
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
