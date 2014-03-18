#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <netdb.h>

#define BUF_SIZE 8192
#define DOWNSTREAM_BUF_SIZE 1450

#define DATA_PORT_INDEX 0
#define HEALTH_PORT_INDEX 1
#define PORTS_NUM 2

int type[] = {SOCK_DGRAM, SOCK_STREAM};

struct downstream_s {
    int health_fd;
    int buffer_length;
    char buffer[DOWNSTREAM_BUF_SIZE];
    struct sockaddr_in *sa_in_data;
    struct sockaddr_in *sa_in_health;
    long last_flush_time_ms;
};

struct config_s {
    int port[PORTS_NUM];
    int server_handle[PORTS_NUM];
    int max_server_handle;
    int downstream_num;
    struct downstream_s *downstream;
    int downstream_health_check_interval;
    int downstream_flush_interval;
    int send_fd;
};

struct config_s config;

unsigned long long get_ms_since_epoch() {
    struct timeval tv;
    gettimeofday(&tv, NULL);

    unsigned long long ms_since_epoch = (unsigned long long)(tv.tv_sec) * 1000 + (unsigned long long)(tv.tv_usec) / 1000;

    return  ms_since_epoch;
}

int flush_downstream(int k) {
    int failures = 0;
    int n = sendto(config.send_fd, config.downstream[k].buffer, config.downstream[k].buffer_length, 0, (struct sockaddr *) (config.downstream[k].sa_in_data), sizeof(*config.downstream[k].sa_in_data));
    if (n < 0) {
        perror("push_to_downstream: sendto() failed");
        failures++;
    }
    config.downstream[k].buffer_length = 0;
    config.downstream[k].buffer[0] = 0;
    config.downstream[k].last_flush_time_ms = get_ms_since_epoch();
    return failures;
}

int push_to_downstream(char *line, unsigned long hash) {
    int downstreams_down[config.downstream_num];
    int downstream_down_length = 0;
    int i, j, k, n;
    for (i = 0; i < config.downstream_num; i++) {
        k = hash % (config.downstream_num - downstream_down_length);
        for (j = 0; j < downstream_down_length; j++) {
            if (k >= downstreams_down[j]) {
                k++;
            }
        }
        if (config.downstream[k].health_fd > 0) {
            j = strlen(line);
            if (config.downstream[k].buffer_length + j > DOWNSTREAM_BUF_SIZE) {
                flush_downstream(k);
            }
            strcpy(config.downstream[k].buffer + config.downstream[k].buffer_length, line);
            config.downstream[k].buffer_length += j;
            config.downstream[k].buffer[config.downstream[k].buffer_length++] = '\n';
            return 0;
        }
        downstreams_down[downstream_down_length++] = k;
    }
    return 1;
}

unsigned long hash(char *s, int length) {
    unsigned long h = 0;
    int i;
    for (i = 0; i < length; i++) {
        h = h * 31 + *(s + i);
    }
    return h;
}

int process_line(char *line) {
    char *colon_ptr = strchr(line, ':');
    if (colon_ptr == NULL) {
        printf("process_line: invalid metric %s\n", line);
        return 1;
    }
    push_to_downstream(line, hash(line, (colon_ptr - line)));
    return 0;
}

int process_data(int server_handle, struct sockaddr_in client_address) {
    socklen_t client_length;
    char buffer[BUF_SIZE];
    int bytes_received;
    char *buffer_ptr = buffer;
    char *delimiter_ptr = NULL;

    client_length = sizeof(client_address);
    if ((bytes_received = recvfrom(server_handle, buffer, sizeof(buffer), 0, (struct sockaddr *)&client_address, &client_length)) < 0) {
        perror("process_data: recvfrom() failed");
        return 1;
    }
    buffer[bytes_received] = 0;
    while (1) {
        delimiter_ptr = strchr(buffer_ptr, '\n');
        if (delimiter_ptr != NULL) {
            *delimiter_ptr = 0;
        }
        if (strlen(buffer_ptr) > 0) {
            process_line(buffer_ptr);
        }
        if (delimiter_ptr == NULL) {
            break;
        }
        buffer_ptr = ++delimiter_ptr;
    }
    return 0;
}

int process_health_check(int server_handle, struct sockaddr_in client_address) {
    socklen_t client_length;
    int childfd;
    char buffer[BUF_SIZE];
    int bytes_received;

    client_length = sizeof(client_address);
    childfd = accept(server_handle, (struct sockaddr *) &client_address, &client_length);
    if (childfd < 0) {
        perror("process_health_check: accept() failed");
        return 1;
    }
    bytes_received = read(childfd, buffer, sizeof(buffer));
    buffer[bytes_received] = 0;
    if (bytes_received < 0) {
        perror("process_health_check: read() failed");
        close(childfd);
        return 2;
    }
    bytes_received = write(childfd, buffer, strlen(buffer));
    if (bytes_received < 0) {
        perror("process_health_check: write() failed");
        close(childfd);
        return 3;
    }
    close(childfd);
    return 0;
}

struct sockaddr_in *get_sockaddr_in(char *host, char *port_s) {
    int port = atoi(port_s);
    struct sockaddr_in *sa_in = malloc(sizeof(struct sockaddr_in));
    struct hostent *he = gethostbyname(host);

    if (port <= 0) {
        printf("get_sockaddr_in: invalid port %s for host %s\n", port_s, host);
        return NULL;
    }
    if (he == NULL || he->h_addr_list == NULL || (he->h_addr_list)[0] == NULL ) {
        perror("get_sockaddr_in: gethostbyname() failed");
        return NULL;
    }
    if (sa_in == NULL) {
        perror("get_sockaddr_in: malloc() failed");
        return NULL;
    }
    bzero(sa_in, sizeof(*sa_in));
    sa_in->sin_family = AF_INET;
    sa_in->sin_port = htons(port);
    memcpy(&(sa_in->sin_addr), he->h_addr_list[0], he->h_length);
    return sa_in;
}

int init_downstream(char *hosts) {
    int i = 0;
    char *host = hosts;
    char *next_host = NULL;
    char *data_port = NULL;
    char *health_port = NULL;

    config.downstream_num = 1;
    while (hosts[i] != 0) {
        if (hosts[i++] == ',') {
            config.downstream_num++;
        }
    }
    config.downstream = (struct downstream_s *)malloc(sizeof(struct downstream_s) * config.downstream_num);
    if (config.downstream == NULL) {
        perror("init_downstream: malloc() failed");
        return 1;
    }
    for (i = 0; i < config.downstream_num; i++) {
        config.downstream[i].buffer_length = 0;
        config.downstream[i].buffer[0] = 0;
        config.downstream[i].health_fd = -1;
        config.downstream[i].last_flush_time_ms = get_ms_since_epoch();
        if (host == NULL) {
            printf("init_downstream: null hostname at iteration %d\n", i);
            return 1;
        }
        next_host = strchr(host, ',');
        if (next_host != NULL) {
            *next_host++ = 0;
        }
        data_port = strchr(host, ':');
        if (data_port == NULL) {
            printf("init_downstream: no data port for %s\n", host);
            return 1;
        }
        *data_port++ = 0;
        health_port = strchr(data_port, ':');
        if (health_port == NULL) {
            printf("init_downstream: no health_port for %s\n", host);
            return 1;
        }
        *health_port++ = 0;
        config.downstream[i].sa_in_data = get_sockaddr_in(host, data_port);
        if (config.downstream[i].sa_in_data == NULL) {
            return 1;
        }
        config.downstream[i].sa_in_health = get_sockaddr_in(host, health_port);
        if (config.downstream[i].sa_in_health == NULL) {
            return 1;
        }
        host = next_host;
    }
    return 0;
}

int process_config_line(char *line) {
    char *value_ptr = strchr(line, '=');
    if (value_ptr == NULL) {
        printf("process_config_line: bad line in config \"%s\"\n", line);
        return 1;
    }
    *value_ptr++ = 0;
    if (strcmp("data_port", line) == 0) {
        config.port[DATA_PORT_INDEX] = atoi(value_ptr);
    } else if (strcmp("health_port", line) == 0) {
        config.port[HEALTH_PORT_INDEX] = atoi(value_ptr);
    } else if (strcmp("downstream_health_check_interval", line) == 0) {
        config.downstream_health_check_interval = atoi(value_ptr);
        config.downstream_flush_interval = config.downstream_health_check_interval;
    } else if (strcmp("downstream", line) == 0) {
        return init_downstream(value_ptr);
    } else {
        printf("process_config_line: unknown parameter \"%s\"\n", line);
        return 1;
    }
    return 0;
}

int init_config(char *filename) {
    int i = 0;
    size_t n = 0;
    int failures = 0;
    char *buffer;

    FILE *config_file = fopen(filename, "rt");
    if (config_file == NULL) {
        perror("init_config: fopen() failed");
        return 1;
    }
    while (getline(&buffer, &n, config_file) > 0) {
        if (strlen(buffer) != 0 && buffer[0] != '#') {
            failures += process_config_line(buffer);
        }
    }
    free(buffer);
    fclose(config_file);
    if (failures > 0) {
        printf("init_config: failed to load config\n");
        return 1;
    }
    for (i = 0; i < PORTS_NUM; i++) {
        config.server_handle[i] = 0;
    }
    config.max_server_handle = 0;
    config.send_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (config.send_fd < 0) {
        perror("init_config: socket() failed");
        return 1;
    }
    return 0;
}

int init_sockets() {
    struct sockaddr_in server_address[PORTS_NUM];
    int i;

    for (i = 0; i < PORTS_NUM; i++) {
        config.server_handle[i] = socket(AF_INET, type[i], 0);
        if (config.server_handle[i] < 0) {
            perror("init_sockets: socket() failed");
            return 1;
        }

        if (config.server_handle[i] > config.max_server_handle)
            config.max_server_handle = config.server_handle[i];

        memset( &server_address[i], 0, sizeof( server_address[i] ) );
        server_address[i].sin_family = AF_INET;
        server_address[i].sin_addr.s_addr = htonl(INADDR_ANY);
        server_address[i].sin_port = htons(config.port[i]);

        if (bind(config.server_handle[i], (struct sockaddr *)&server_address[i], sizeof( server_address[i] )) < 0) {
            perror("init_sockets: bind() failed");
            return 1;
        }

        if (type[i] == SOCK_STREAM && listen(config.server_handle[i], 5) < 0) { /* allow 5 requests to queue up */ 
            perror("init_sockets: listen() failed");
            return 1;
        }
    }
    return 0;
}

void mark_downstream_down(int i) {
    if (config.downstream[i].health_fd > 0) {
        close(config.downstream[i].health_fd);
        config.downstream[i].health_fd = -1;
    }
    config.downstream[i].buffer_length = 0;
    config.downstream[i].buffer[0] = 0;
}

int run_downstream_health_check() {
    int i, n;
    int failures = 0;
    char buffer[BUF_SIZE];
    char *health_check_request = "health";
    int health_check_request_length = strlen(health_check_request);

    for (i = 0; i < config.downstream_num; i++) {
        if (config.downstream[i].health_fd < 0) {
            config.downstream[i].health_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (config.downstream[i].health_fd < 0) {
                failures++;
                mark_downstream_down(i);
//                perror("socket() failed");
                continue;
            }
            if (connect(config.downstream[i].health_fd, (struct sockaddr*) (config.downstream[i].sa_in_health), sizeof(*config.downstream[i].sa_in_health)) < 0) {
                failures++;
//                perror("connect() failed");
                mark_downstream_down(i);
                continue;
            }
        }
	n = send(config.downstream[i].health_fd, health_check_request, health_check_request_length, 0);
        if (n <= 0) {
            failures++;
//            perror("send() failed");
            mark_downstream_down(i);
            continue;
        }
        n = recv(config.downstream[i].health_fd, buffer, BUF_SIZE, 0);
        if (n <= 0) {
            failures++;
//            perror("recv() failed");
            mark_downstream_down(i);
            continue;
        }
    }
    return failures;
}

int run_downstream_flush(long current_time_ms) {
    int i;
    int failures = 0;
    for (i = 0; i < config.downstream_num; i++) {
        if (current_time_ms - config.downstream[i].last_flush_time_ms > config.downstream_flush_interval && config.downstream[i].buffer_length > 0) {
            failures += flush_downstream(i);
        }
    }
    return failures;
}

void main_loop() {
    fd_set read_handles;
    int i;
    struct timeval timeout_interval;
    int retval;
    struct sockaddr_in client_address[PORTS_NUM];
    long last_ds_health_check = 0;
    int select_timeout = config.downstream_health_check_interval / 10;

    while (1) {
        long current_time_ms = get_ms_since_epoch();
        run_downstream_flush(current_time_ms);
        if (current_time_ms - last_ds_health_check > config.downstream_health_check_interval) {
            run_downstream_health_check(current_time_ms);
            last_ds_health_check = current_time_ms;
        }
        FD_ZERO(&read_handles);
        for (i = 0; i < PORTS_NUM; i++)
            FD_SET(config.server_handle[i], &read_handles);

        timeout_interval.tv_sec = select_timeout / 1000;
        timeout_interval.tv_usec = (select_timeout % 1000) * 1000;

        retval = select(config.max_server_handle + 1, &read_handles, NULL, NULL, &timeout_interval);
        if (retval == -1) {
            printf("main_loop: select() failed\n");
            //error
        } else if (retval == 0) {
//            printf("timeout\n");
        } else {
            //good
            for (i = 0; i < PORTS_NUM; i++) {
                if (FD_ISSET(config.server_handle[i], &read_handles)) {
                    if (type[i] == SOCK_DGRAM) {
                        process_data(config.server_handle[i], client_address[i]);
                    }
                    if (type[i] == SOCK_STREAM) {
                        process_health_check(config.server_handle[i], client_address[i]);
                    }
                }
            }
        }
    }
}

int main(int argc, char **argv) {
    if (argc != 2) {
        printf("Usage: %s config.file\n", argv[0]);
        exit(0);
    }
    if (init_config(argv[1]) != 0) {
        printf("init_config() failed\n");
        exit(1);
    }
    if (init_sockets() != 0) {
        printf("init_sockets() failed\n");
        exit(1);
    }
    main_loop();
}
