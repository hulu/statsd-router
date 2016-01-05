#include "sr-main.h"

#define HOST_NAME_SIZE 64

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
static int init_downstream(sr_config_s *config) {
    int i = 0;
    int j = 0;
    int k = 0;
    char *hosts = config->downstream_str;
    char *host = hosts;
    char *next_host = NULL;
    char *data_port = NULL;
    char *health_port = NULL;
    char per_connection_prefix[METRIC_SIZE];
    char per_downstream_prefix[METRIC_SIZE];
    char metric_host_name[METRIC_SIZE];
    struct downstream_s *ds;

    // argument line has the following format: host1:data_port1:health_port1,host2:data_port2:healt_port2,...
    // number of downstreams is equal to number of commas + 1
    config->downstream_num = 1;
    while (hosts[i] != 0) {
        if (hosts[i++] == ',') {
            config->downstream_num++;
        }
    }
    config->downstream = (struct downstream_s *)malloc(sizeof(struct downstream_s) * config->downstream_num * config->threads_num);
    if (config->downstream == NULL) {
        log_msg(ERROR, "%s: downstream malloc() failed %s", __func__, strerror(errno));
        return 1;
    }
    config->health_client = (struct ds_health_client_s *)malloc(sizeof(struct ds_health_client_s) * config->downstream_num);
    if (config->health_client == NULL) {
        log_msg(ERROR, "%s: health client malloc() failed %s", __func__, strerror(errno));
        return 1;
    }
    strcpy(per_connection_prefix, config->alive_downstream_metric_name);
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
    // now let's initialize downstreams and health clients
    for (i = 0; i < config->downstream_num; i++) {
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

        (config->health_client + i)->super.fd = -1;
        (config->health_client + i)->id = i;
        (config->health_client + i)->alive = 0;
        if (init_sockaddr_in(&((config->health_client + i)->sa_in), host, health_port) != 0) {
            return 1;
        }

        for (j = 0; *(host + j) != 0; j++) {
            if (*(host + j) == '.') {
                *(metric_host_name + j) = '_';
            } else {
                *(metric_host_name + j) = *(host + j);
            }
        }
        for (k = 0; k < config->threads_num; k++) {
            ds = config->downstream + k * config->downstream_num + i;
            ds->last_flush_time = ev_time();
            ds->active_buffer_idx = 0;
            ds->active_buffer = ds->buffer;
            ds->active_buffer_length = 0;
            ds->flush_buffer_idx = 0;
            ds->downstream_traffic_counter = 0;
            ds->downstream_packet_counter = 0;
            ds->health_client = config->health_client + i;
            for (j = 0; j < DOWNSTREAM_BUF_NUM; j++) {
                ds->buffer_length[j] = 0;
            }
            if (init_sockaddr_in(&(ds->sa_in_data), host, data_port) != 0) {
                return 1;
            }
            ds->per_downstream_counter_metric_length = sprintf(ds->per_downstream_counter_metric, "%s-%s-%s.%s\n%s.%s-%s.%s\n",
                per_connection_prefix, metric_host_name, data_port, PER_DOWNSTREAM_COUNTER_METRIC_SUFFIX,
                per_downstream_prefix, metric_host_name, data_port, PER_DOWNSTREAM_COUNTER_METRIC_SUFFIX);
            sprintf(ds->downstream_packet_counter_metric, "%s.%s-%s.%s",
                per_downstream_prefix, metric_host_name, data_port, DOWNSTREAM_PACKET_COUNTER);
            sprintf(ds->downstream_traffic_counter_metric, "%s.%s-%s.%s",
                per_downstream_prefix, metric_host_name, data_port, DOWNSTREAM_TRAFFIC_COUNTER);
        }
        host = next_host;
    }
    return 0;
}

// function to parse single line from config file
static int process_config_line(char *line, sr_config_s *config) {
    int n;
    // valid line should contain '=' symbol
    char *value_ptr = strchr(line, '=');
    if (value_ptr == NULL) {
        log_msg(ERROR, "%s: bad line in config \"%s\"", __func__, line);
        return 1;
    }
    *value_ptr++ = 0;
    if (strcmp("data_port", line) == 0) {
        config->data_port = atoi(value_ptr);
    } else if (strcmp("control_port", line) == 0) {
        config->control_port = atoi(value_ptr);
    } else if (strcmp("downstream_flush_interval", line) == 0) {
        config->downstream_flush_interval = atof(value_ptr);
    } else if (strcmp("downstream_health_check_interval", line) == 0) {
        config->downstream_health_check_interval = atof(value_ptr);
    } else if (strcmp("downstream_ping_interval", line) == 0) {
        config->downstream_ping_interval = atof(value_ptr);
    } else if (strcmp("log_level", line) == 0) {
        log_level = atoi(value_ptr);
    } else if (strcmp("threads_num", line) == 0) {
        config->threads_num = atoi(value_ptr);
    } else if (strcmp("ping_prefix", line) == 0) {
        n = strlen(value_ptr);
        config->ping_prefix = (char *)malloc(n);
        if (config->ping_prefix == NULL) {
            log_msg(ERROR, "%s: malloc() failed", __func__);
            return 1;
        }
        strncpy(config->ping_prefix, value_ptr, n);
    } else if (strcmp("downstream", line) == 0) {
        n = strlen(value_ptr);
        config->downstream_str = (char *)malloc(n);
        if (config->downstream_str == NULL) {
            log_msg(ERROR, "%s: malloc() failed", __func__);
            return 1;
        }
        strncpy(config->downstream_str, value_ptr, n);
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

static int verify_config(sr_config_s *config) {
    int failures = 0;
    if (config->data_port == 0) {
        failures++;
        log_msg(ERROR, "%s: data_port not set", __func__);
    }
    if (config->control_port == 0) {
        failures++;
        log_msg(ERROR, "%s: control_port not set", __func__);
    }
    if (log_level < TRACE || log_level > ERROR) {
        failures++;
        log_msg(ERROR, "%s: log_level should be in the %d-%d range", __func__, TRACE, ERROR);
    }
    if (config->threads_num < 1) {
        failures++;
        log_msg(ERROR, "%s: threads_num should be > 0", __func__);
    }
    if (config->downstream_str == NULL) {
        failures++;
        log_msg(ERROR, "%s: downstream is not set", __func__);
    }
    if (config->ping_prefix == NULL) {
        failures++;
        log_msg(ERROR, "%s: ping_prefix is not set", __func__);
    }
    if (config->downstream_health_check_interval < 0) {
        failures++;
        log_msg(ERROR, "%s: downstream_health_check_interval should be > 0", __func__);
    }
    if (config->downstream_flush_interval < 0) {
        failures++;
        log_msg(ERROR, "%s: downstream_flush_interval should be > 0", __func__);
    }
    if (config->downstream_ping_interval < 0) {
        failures++;
        log_msg(ERROR, "%s: downstream_ping_interval should be > 0", __func__);
    }
    return failures;
}

// this function loads config file and initializes config fields
int init_config(char *filename, sr_config_s *config) {
    size_t n = 0;
    int l = 0;
    int failures = 0;
    char *buffer;
    char hostname[HOST_NAME_SIZE];

    config->data_port = 0;
    config->control_port = 0;
    log_level = 0;
    config->downstream_health_check_interval = -1.0;
    config->downstream_flush_interval = -1.0;
    config->downstream_ping_interval = -1.0;
    config->threads_num = 0;
    config->downstream_str = NULL;
    config->ping_prefix = NULL;

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
            failures += process_config_line(buffer, config);
        }
    }
    // buffer is reused by getline() so we need to free it only once
    free(buffer);
    fclose(config_file);
    if (failures > 0) {
        log_msg(ERROR, "%s: failed to load config file", __func__);
        return 1;
    }
    if (verify_config(config) != 0) {
        log_msg(ERROR, "%s: failed to verify config file", __func__);
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
    n = gethostname(hostname, HOST_NAME_SIZE);
    if (n < 0) {
        log_msg(ERROR, "%s: gethostname() failed", __func__);
        return 1;
    }
    sprintf(config->alive_downstream_metric_name, "%s.%s-%d.%s", config->ping_prefix, hostname, config->data_port, HEALTHY_DOWNSTREAMS);
    if (init_downstream(config) != 0) {
        log_msg(ERROR, "%s: init_downstream() failed", __func__);
        return 1;
    }
    strncpy(config->health_check_response_buf, HEALTH_CHECK_UP_RESPONSE, STRLEN(HEALTH_CHECK_UP_RESPONSE));
    config->health_check_response_buf_length = STRLEN(HEALTH_CHECK_UP_RESPONSE);
    return 0;
}
