#include "sr-util.h"

// function to convert numeric values into strings
static char *log_level_name(enum log_level_e level) {
    static char *name[] = { "TRACE", "DEBUG", "INFO", "WARN", "ERROR"};
    return name[level];
}

// function to log message
void log_msg(int level, char *format, ...) {
    va_list args;
    time_t t;
    struct tm *tinfo;
    char buffer[LOG_BUF_SIZE];
    int l = 0;

    if (level < log_level) {
        return;
    }
    va_start(args, format);
    time(&t);
    tinfo = localtime(&t);
    l = strftime(buffer, LOG_BUF_SIZE, "%Y-%m-%d %H:%M:%S", tinfo);
    l += sprintf(buffer + l, " %ld %s ", syscall(SYS_gettid), log_level_name(level));
    vsnprintf(buffer + l, LOG_BUF_SIZE - l, format, args);
    va_end(args);
    fprintf(stdout, "%s\n", buffer);
    fflush(stdout);
}

