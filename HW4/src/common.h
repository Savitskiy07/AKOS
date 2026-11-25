#ifndef GALLERY_COMMON_H
#define GALLERY_COMMON_H

#include <stddef.h>
#include <stdint.h>

#define MAX_VISITORS 1000
#define MAX_PICTURES 64
#define MAX_CONNECTIONS 1024
#define LINE_SIZE 1024

typedef struct {
    int fd;
    char data[4096];
    size_t pos;
    size_t len;
} LineReader;

int parse_int(const char *s, int min, int max, int *value);
int parse_u32(const char *s, uint32_t *value);
int write_all(int fd, const void *data, size_t size);
int print_fd(int fd, const char *format, ...);
int send_line(int fd, const char *format, ...);
/* 1: complete line; 0: clean EOF; -1: I/O or framing error. */
int read_line(LineReader *reader, char *line, size_t capacity);
int connect_tcp(const char *host, const char *port);
int listen_tcp(const char *host, const char *port);
int set_nonblocking(int fd);
int64_t monotonic_ms(void);
int sleep_ms(int milliseconds);
uint32_t random_next(uint32_t *state);
int random_between(uint32_t *state, int min, int max);
void ignore_sigpipe(void);

#endif
