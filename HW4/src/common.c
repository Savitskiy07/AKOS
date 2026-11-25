#define _POSIX_C_SOURCE 200809L
#include "common.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

int parse_int(const char *s, int min, int max, int *value) {
    if (!s || !*s) return -1;
    for (const char *p = s; *p; ++p)
        if (*p < '0' || *p > '9') return -1;
    errno = 0;
    char *end;
    long n = strtol(s, &end, 10);
    if (errno || *end || n < min || n > max) return -1;
    *value = (int)n;
    return 0;
}

int parse_u32(const char *s, uint32_t *value) {
    if (!s || !*s) return -1;
    for (const char *p = s; *p; ++p)
        if (*p < '0' || *p > '9') return -1;
    errno = 0;
    char *end;
    unsigned long n = strtoul(s, &end, 10);
    if (errno || *end || n > UINT32_MAX) return -1;
    *value = (uint32_t)n;
    return 0;
}

int write_all(int fd, const void *data, size_t size) {
    const char *p = data;
    while (size) {
        ssize_t n = write(fd, p, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        p += n;
        size -= (size_t)n;
    }
    return 0;
}

int print_fd(int fd, const char *format, ...) {
    char line[4096];
    va_list args;
    va_start(args, format);
    int n = vsnprintf(line, sizeof(line), format, args);
    va_end(args);
    if (n < 0 || (size_t)n >= sizeof(line)) return -1;
    return write_all(fd, line, (size_t)n);
}

int send_line(int fd, const char *format, ...) {
    char line[LINE_SIZE];
    va_list args;
    va_start(args, format);
    int n = vsnprintf(line, sizeof(line), format, args);
    va_end(args);
    if (n < 0 || (size_t)n >= sizeof(line)) return -1;
    size_t offset = 0;
    while (offset < (size_t)n) {
        ssize_t sent = send(fd, line + offset, (size_t)n - offset, MSG_NOSIGNAL);
        if (sent < 0 && errno == EINTR) continue;
        if (sent <= 0) return -1;
        offset += (size_t)sent;
    }
    return 0;
}

int read_line(LineReader *reader, char *line, size_t capacity) {
    size_t used = 0;
    for (;;) {
        if (reader->pos == reader->len) {
            ssize_t n = recv(reader->fd, reader->data, sizeof(reader->data), 0);
            if (n < 0 && errno == EINTR) continue;
            if (n < 0) return -1;
            if (n == 0) return used == 0 ? 0 : -1;
            reader->pos = 0;
            reader->len = (size_t)n;
        }
        char c = reader->data[reader->pos++];
        if (c == '\n') {
            line[used] = '\0';
            return 1;
        }
        if (c == '\0' || used + 1 >= capacity) return -1;
        line[used++] = c;
    }
}

static int open_tcp(const char *host, const char *port, int server) {
    struct addrinfo hints = {0}, *addresses = NULL;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;
    int err = getaddrinfo(host, port, &hints, &addresses);
    if (err) {
        print_fd(STDERR_FILENO, "Address: %s\n", gai_strerror(err));
        errno = EINVAL;
        return -1;
    }
    int fd = -1;
    for (struct addrinfo *a = addresses; a; a = a->ai_next) {
        fd = socket(a->ai_family, a->ai_socktype, a->ai_protocol);
        if (fd < 0) continue;
        int no_delay = 1;
        if (!server && setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &no_delay, sizeof(no_delay)) < 0) {
            close(fd);
            fd = -1;
            continue;
        }
        if (server) {
            int one = 1;
            if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) == 0 &&
                bind(fd, a->ai_addr, a->ai_addrlen) == 0 && listen(fd, 512) == 0)
                break;
        } else if (connect(fd, a->ai_addr, a->ai_addrlen) == 0) {
            break;
        }
        int saved = errno;
        close(fd);
        fd = -1;
        errno = saved;
    }
    freeaddrinfo(addresses);
    return fd;
}

int connect_tcp(const char *host, const char *port) { return open_tcp(host, port, 0); }
int listen_tcp(const char *host, const char *port) { return open_tcp(host, port, 1); }

int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL);
    return flags < 0 ? -1 : fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int64_t monotonic_ms(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) < 0) {
        perror("clock_gettime");
        exit(EXIT_FAILURE);
    }
    return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

int sleep_ms(int milliseconds) {
    struct timespec delay = {milliseconds / 1000, (milliseconds % 1000) * 1000000L};
    while (nanosleep(&delay, &delay) < 0)
        if (errno != EINTR) return -1;
    return 0;
}

uint32_t random_next(uint32_t *state) {
    /* A private PRNG per visitor: scheduling does not change its route. */
    if (*state == 0) *state = UINT32_C(0x9e3779b9);
    uint32_t x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    return x;
}

int random_between(uint32_t *state, int min, int max) {
    return min + (int)(random_next(state) % (uint32_t)(max - min + 1));
}

void ignore_sigpipe(void) {
    struct sigaction action = {0};
    action.sa_handler = SIG_IGN;
    sigemptyset(&action.sa_mask);
    sigaction(SIGPIPE, &action, NULL);
}
