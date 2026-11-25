#define _POSIX_C_SOURCE 200809L
#include "common.h"

#include <getopt.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static void usage(void) {
    print_fd(1, "Usage: gallery_observer --host IP --port PORT\n"
        "Receives an atomic snapshot, then all subsequent gallery events.\n"
        "Press Ctrl+C to disconnect; run again at any time to reconnect.\n");
}

int main(int argc, char **argv) {
    if (argc == 2 && !strcmp(argv[1], "--help")) { usage(); return 0; }
    const char *host = NULL, *port = NULL;
    int port_number, option, bad = 0;
    static const struct option options[] = {
        {"host", required_argument, NULL, 'h'}, {"port", required_argument, NULL, 'p'},
        {NULL, 0, NULL, 0}
    };
    opterr = 0;
    while ((option = getopt_long(argc, argv, "", options, NULL)) != -1) {
        if (option == 'h') host = optarg;
        else if (option == 'p') port = optarg;
        else bad = -1;
    }
    if (bad || optind != argc || !host || !port || parse_int(port, 1, 65535, &port_number)) {
        usage(); print_fd(2, "Invalid arguments\n"); return 1;
    }
    ignore_sigpipe();
    int fd = connect_tcp(host, port);
    if (fd < 0) { perror("connect"); return 1; }
    if (send_line(fd, "HELLO OBSERVER\n")) { perror("send"); close(fd); return 2; }
    LineReader reader = {.fd = fd};
    char line[LINE_SIZE];
    int status = 2;
    print_fd(1, "Наблюдатель подключён. Далее: текущее состояние и события всей галереи.\n");
    while (read_line(&reader, line, sizeof(line)) == 1) {
        if (print_fd(1, "%s\n", line)) break;
        if (!strncmp(line, "ERROR ", 6)) break;
        if (!strncmp(line, "STOP ", 5)) {
            status = !strcmp(line, "STOP reason=complete") ? 0 :
                (!strcmp(line, "STOP reason=interrupt") ? 130 : 2);
            break;
        }
    }
    if (status == 2) print_fd(2, "Observer ended without successful completion; reconnect if the server is still running.\n");
    close(fd);
    return status;
}
