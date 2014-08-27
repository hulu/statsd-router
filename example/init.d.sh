#!/bin/bash

# HUP is ignored so that remote starts via ssh wouldn't be killed on ssh connection close
trap '' HUP

# Variables can't be unbound, any error cause immediate exit
set -u -e

# location of statsd-router binary
STATSD_ROUTER=/usr/local/bin/statsd-router
# location of statsd
STATSD=/opt/statsd/bin/statsd
# how many statsd-router instances do we run
STATSD_ROUTER_INSTANCES=2
# statsd-router uses ports 8100, 8101 etc
BASE_STATSD_ROUTER_PORT=8100
# statsd uses ports 8200, 8201 etc
BASE_STATSD_PORT=8200
# how often we check that cluster components are running
WATCHDOG_INTERVAL=5 # seconds
# location of configs for statsd and statsd-router instances
CONF_DIR=/tmp
# location of logs
LOG_DIR=/tmp
# location of file with statsd endpoints
STATSD_ENDPOINTS_FILE=$CONF_DIR/statsd-endpoints

full_name=$(cd $(dirname $0) && echo $(pwd)/$(basename $0)) # let's get full name of this script
canonical_name=$(readlink -e $0) # let's get real name if script was started via symlink

if [ "$canonical_name" != "$full_name" ] ; then # if script was started via symlink ...
    $canonical_name $@                          # ... start it using real name
    exit                                        # this is necessary in order for stop() function to work
fi

# number of cpus on this box
cpus=$(grep ^processor /proc/cpuinfo|wc -l)

# various program specfic parameters
# number of statsd instances is number of cpus minus number of statsd-router instances
# there is no cpu over subscription
declare -A run_data=(
    [statsd-router]="${BASE_STATSD_ROUTER_PORT},${STATSD_ROUTER_INSTANCES},ok,ok"
    [statsd]="${BASE_STATSD_PORT},$((cpus - STATSD_ROUTER_INSTANCES)),health,health: up"
)

# function to create config for statsd-router
create_statsd_router_config() {
    local port=$1
    cat <<EOF_STATSD_ROUTER_CONFIG
log_level=3
data_port=${port}
health_port=${port}
downstream_health_check_interval=2.0
downstream_flush_interval=2.0
downstream_ping_interval=1.0
ping_prefix=statsd-cluster
downstream=$(cat $STATSD_ENDPOINTS_FILE)
EOF_STATSD_ROUTER_CONFIG
}

# function to create config for statsd
create_statsd_config() {
    local port=$1
    cat <<EOF_STATSD_CONFIG
{
    graphitePort: 2014,
    graphiteHost: "127.0.0.1",
    port: ${port},
    mgmt_port: ${port},
    percentThreshold: [50, 75, 90, 95, 99],
    backends: ['./backends/graphite'],
    deleteIdleStats: true,
    deleteCounters: true,
    deleteTimers: true,
    deleteGauge: true,
    deleteSets: true,
    log: {backend: "syslog", level: "LOG_ERR"}
}
EOF_STATSD_CONFIG
}

# start statsd if it is not running
check_statsd() {
    local port=$1 conf=$2
    [ -r "$conf" ] || {
        create_statsd_config $port >$conf
        [ -r "$LOG_DIR/statsd-${port}.log" ] && mv -f $LOG_DIR/statsd-${port}.log $LOG_DIR/statsd-${port}.log.old
        nohup $STATSD $conf >$LOG_DIR/statsd-${port}.log 2>&1 &
    }
}

# (re)start statsd-router if it is not running or if endpoints changed
check_statsd_router() {
    local port=$1 conf=$2
    [ -r "$conf" ] || {
        create_statsd_router_config $port >$conf
        nohup $STATSD_ROUTER $conf 2>&1 | logger -t statsd-router &
    }
}

# kill process by name and port number
kill_by_name_and_port() {
    local name=$1 port=$2 pids
    # port var can contain several ports like this: 8200|8201|8202
    pids=$(netstat -lntp | grep -P "^tcp\s+\d+\s+\d+\s+0.0.0.0:($port)\s+0.0.0.0:\*\s+LISTEN\s+\d+/$name\s*$"|awk '{print $7}'|cut -f1 -d/)
    [ -z "$pids" ] && return 0
    kill $pids
}

# watchdog that runs every WATCHDOG_INTERVAL seconds and checks health of cluster components
watchdog() {
    local port conf name base_port instances health_request health_response statsd_endpoints_updated

    # now let's cycle through all cluster components
    for name in ${!run_data[@]}; do
        base_port=$(echo ${run_data[$name]}|cut -f1 -d,)
        instances=$(echo ${run_data[$name]}|cut -f2 -d,)
        health_request=$(echo ${run_data[$name]}|cut -f3 -d,)
        health_response=$(echo ${run_data[$name]}|cut -f4 -d,)
        for ((i = 0; i < instances; i++)) {
            port=$((base_port + i))
            conf=$CONF_DIR/${name}-${port}.conf
            # lets check health of the component
            if [ "$(echo $health_request|nc -w1 127.0.0.1 $port)" != "$health_response" ] ; then
                # if no expected response was received we delete config
                rm -f $conf
                # and kill instance if it is still running
                kill_by_name_and_port $name $port
            fi
            # now let's check if we need (re)start cluster components
            check_${name//-/_} $port $conf $statsd_endpoints_updated
        }
    done

    sleep $WATCHDOG_INTERVAL
    $full_name watchdog &
}

# function to start cluster creates list of endpoints and  forks watchdog
start() {
    statsd_instances=$((cpus - STATSD_ROUTER_INSTANCES))
    statsd_endpoints=""
    # information on endpoints is appended to environment variable
    for ((i = 0; i < statsd_instances; i++)) {
        statsd_endpoints+=${statsd_endpoints:+,}127.0.0.1:$((BASE_STATSD_PORT+i)):$((BASE_STATSD_PORT+i))
    }
    echo $statsd_endpoints > $STATSD_ENDPOINTS_FILE
    $0 watchdog &
    echo "$(basename $0) watchdog started"
}

# function to stop cluster
stop() {
    local i name base_port ports="" instances
    # let's kill watchdog
    pkill -f "$(basename $0).*watchdog" || true
    # now let's cycle through cluster components
    for name in ${!run_data[@]}; do
        base_port=$(echo ${run_data[$name]}|cut -f1 -d,)
        instances=$(echo ${run_data[$name]}|cut -f2 -d,)
        # let's create list of ports for the component
        for ((i = 0; i < instances; i++)) {
            ports+=${ports:+|}$((base_port + i))
        }
        # and let's kill them
        kill_by_name_and_port $name "$ports"
    done
    echo "$(basename $0) stopped"
}

case ${1:-} in
    watchdog) watchdog ;;
    start) start ;;
    stop) stop ;;
    restart) stop && start ;;
    *) echo "Usage: $(basename $0) start|stop|restart"
esac

