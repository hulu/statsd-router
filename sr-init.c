#include "sr-main.h"

static int init_sockaddr_in(struct sockaddr_in *sa_in, char *host, char *port) {
    struct hostent *he = gethostbyname(host);

    if (he == NULL || he->h_addr_list == NULL || (he->h_addr_list)[0] == NULL ) {
        log_msg(ERROR, "%s: gethostbyname() failed %s", __func__, strerror(errno));
        return 1;
    }
    bzero(sa_in, sizeof(*sa_in));
    sa_in->sin_family = AF_INET;
    sa_in->sin_port = htons(atoi(port));
    memcpy(&(sa_in->sin_addr), he->h_addr_list[0], he->h_length);
    return 0;
}

// function to init downstreams from config file line
static int init_downstream(char *hosts) {
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
        log_msg(ERROR, "%s: malloc() failed %s", __func__, strerror(errno));
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
        global.downstream[i].active_buffer_idx = 0;
        global.downstream[i].active_buffer = global.downstream[i].buffer;
        global.downstream[i].active_buffer_length = 0;
        global.downstream[i].flush_buffer_idx = 0;
        global.downstream[i].health_watcher.super.fd = -1;
        global.downstream[i].health_watcher.id = i;
        global.downstream[i].flush_watcher.super.fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);;
        global.downstream[i].flush_watcher.id = i;
        global.downstream[i].downstream_traffic_counter = 0;
        global.downstream[i].downstream_packet_counter = 0;
        global.downstream[i].alive = 0;
        for (j = 0; j < DOWNSTREAM_BUF_NUM; j++) {
            global.downstream[i].buffer_length[j] = 0;
        }
        if (global.downstream[i].flush_watcher.super.fd < 0) {
            log_msg(ERROR, "%s: socket() failed %s", __func__, strerror(errno));
            return 1;
        }
        if (host == NULL) {
            log_msg(ERROR, "%s: null hostname at iteration %d", __func__, i);
            return 1;
        }
        next_host = strchr(host, ',');
        if (next_host != NULL) {
            *next_host++ = 0;
        }
        data_port = strchr(host, ':');
        if (data_port == NULL) {
            log_msg(ERROR, "%s: no data port for %s", __func__, host);
            return 1;
        }
        *data_port++ = 0;
        health_port = strchr(data_port, ':');
        if (health_port == NULL) {
            log_msg(ERROR, "%s: no health_port for %s", __func__, host);
            return 1;
        }
        *health_port++ = 0;
        if (init_sockaddr_in(&global.downstream[i].sa_in_data, host, data_port) != 0) {
            return 1;
        }
        if (init_sockaddr_in(&global.downstream[i].sa_in_health, host, health_port) != 0) {
            return 1;
        }
        for (j = 0; *(host + j) != 0; j++) {
            if (*(host + j) == '.') {
                *(host + j) = '_';
            }
        }
        sprintf(global.downstream[i].per_downstream_counter_metric, "%s-%s-%s.%s\n%s.%s-%s.%s\n",
            per_connection_prefix, host, data_port, PER_DOWNSTREAM_COUNTER_METRIC_SUFFIX,
            per_downstream_prefix, host, data_port, PER_DOWNSTREAM_COUNTER_METRIC_SUFFIX);
        global.downstream[i].per_downstream_counter_metric_length = strlen(global.downstream[i].per_downstream_counter_metric);
        sprintf(global.downstream[i].downstream_packet_counter_metric, "%s.%s-%s.%s",
            per_downstream_prefix, host, data_port, DOWNSTREAM_PACKET_COUNTER);
        sprintf(global.downstream[i].downstream_traffic_counter_metric, "%s.%s-%s.%s",
            per_downstream_prefix, host, data_port, DOWNSTREAM_TRAFFIC_COUNTER);
        host = next_host;
    }
    return 0;
}

// function to parse single line from config file
static int process_config_line(char *line) {
    char buffer[METRIC_SIZE];
    int n;
    // valid line should contain '=' symbol
    char *value_ptr = strchr(line, '=');
    if (value_ptr == NULL) {
        log_msg(ERROR, "%s: bad line in config \"%s\"", __func__, line);
        return 1;
    }
    *value_ptr++ = 0;
    if (strcmp("data_port", line) == 0) {
        global.port[DATA_PORT_INDEX] = atoi(value_ptr);
    } else if (strcmp("control_port", line) == 0) {
        global.port[CONTROL_PORT_INDEX] = atoi(value_ptr);
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
            log_msg(ERROR, "%s: gethostname() failed", __func__);
            return 1;
        }
        sprintf(global.alive_downstream_metric_name, "%s.%s-%d.%s", value_ptr, buffer, global.port[DATA_PORT_INDEX], HEALTHY_DOWNSTREAMS);
    } else if (strcmp("downstream", line) == 0) {
        return init_downstream(value_ptr);
    } else {
        log_msg(ERROR, "%s: unknown parameter \"%s\"", __func__, line);
        return 1;
    }
    return 0;
}

// this function is called if SIGHUP is received
static void on_sighup(int sig) {
    log_msg(INFO, "%s: sighup received", __func__);
}

// this function is called if SIGINT is received
static void on_sigint(int sig) {
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
        log_msg(ERROR, "%s: signal() for sighup failed", __func__);
        return 1;
    }
    if (signal(SIGINT, on_sigint) == SIG_ERR) {
        log_msg(ERROR, "%s: signal() for sigint failed", __func__);
        return 1;
    }
    strncpy(global.health_check_response_buf, HEALTH_CHECK_UP_RESPONSE, STRLEN(HEALTH_CHECK_UP_RESPONSE));
    global.health_check_response_buf_length = STRLEN(HEALTH_CHECK_UP_RESPONSE);
    return 0;
}
