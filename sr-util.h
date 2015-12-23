#ifndef _SR_UTIL_H
#define _SR_UTIL_H

#include <time.h>
#include <stdio.h>
#include <stdarg.h>

#include "sr-types.h"

#define LOG_BUF_SIZE 2048

// numeric values for log levels
enum log_level_e {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR
};

void log_msg(int level, char *format, ...);

#endif
