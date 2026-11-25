#define _POSIX_C_SOURCE 200809L
#include "common.h"

#include <getopt.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static void usage(void) {
    print_fd(1, "Usage: gallery_visitor --host IP --port PORT --id N [--arrival-ms N]\n"
        "  --id N          unique ID in 1..server's --visitors\n"
        "  --arrival-ms N  delay before arrival, 0..3600000, default 0\n");
}

static int visit(int fd, int id) {
    LineReader reader = {.fd = fd};
    char line[LINE_SIZE];
    int route[MAX_PICTURES], pictures = 0, completed = 0;
    bool configured = false, entered = false, viewing = false;
    if (send_line(fd, "HELLO VISITOR %d\n", id)) return 2;
    for (;;) {
        int result = read_line(&reader, line, sizeof(line));
        if (result != 1) {
            print_fd(2, "VISITOR id=%d: connection ended before BYE\n", id);
            return 2;
        }
        print_fd(1, "VISITOR id=%d <- %s\n", id, line);
        if (!strncmp(line, "ERROR ", 6)) return 2;
        if (!strncmp(line, "STOP ", 5))
            return !strcmp(line, "STOP reason=interrupt") ? 130 : 2;
        char strategy[17], extra;
        unsigned int seed;
        int p, duration, count;
        if (!configured && sscanf(line, "CONFIG %d %16s %u %c", &count, strategy, &seed, &extra) == 3) {
            if (count < 1 || count > MAX_PICTURES ||
                (strcmp(strategy, "random") && strcmp(strategy, "sequential"))) return 2;
            pictures = count;
            for (int i = 0; i < pictures; ++i) route[i] = i + 1;
            if (!strcmp(strategy, "random")) {
                uint32_t rng = (uint32_t)seed ^ UINT32_C(0x85ebca6b);
                /* Fisher-Yates gives a route containing each picture once. */
                for (int i = pictures - 1; i > 0; --i) {
                    int j = random_between(&rng, 0, i);
                    int tmp = route[i]; route[i] = route[j]; route[j] = tmp;
                }
            }
            configured = true;
            continue;
        }
        if (configured && !entered && !strcmp(line, "WAIT_ENTRY")) {
            print_fd(1, "Посетитель %d ожидает разрешения вахтёра.\n", id);
            continue;
        }
        if (configured && !entered && !strcmp(line, "ENTERED")) {
            entered = true;
            print_fd(1, "Посетитель %d вошёл; выбирает картину %d.\n", id, route[0]);
            if (send_line(fd, "VIEW %d\n", route[0])) return 2;
            continue;
        }
        if (entered && completed < pictures && !viewing &&
            sscanf(line, "WAIT_PICTURE %d %c", &p, &extra) == 1 && p == route[completed]) {
            print_fd(1, "Посетитель %d ожидает место возле картины %d.\n", id, p);
            continue;
        }
        if (entered && completed < pictures && !viewing &&
            sscanf(line, "START %d %d %c", &p, &duration, &extra) == 2 &&
            p == route[completed] && duration >= 0 && duration <= 60000) {
            viewing = true;
            print_fd(1, "Посетитель %d осматривает картину %d (%d мс).\n", id, p, duration);
            continue;
        }
        if (viewing && completed < pictures &&
            sscanf(line, "DONE %d %c", &p, &extra) == 1 && p == route[completed]) {
            viewing = false;
            ++completed;
            print_fd(1, "Посетитель %d закончил осмотр картины %d (%d/%d).\n", id, p, completed, pictures);
            if (completed == pictures) {
                if (send_line(fd, "LEAVE\n")) return 2;
            } else if (send_line(fd, "VIEW %d\n", route[completed])) return 2;
            continue;
        }
        if (entered && completed == pictures && !viewing && !strcmp(line, "BYE")) {
            print_fd(1, "Посетитель %d осмотрел все картины и вышел.\n", id);
            return 0;
        }
        print_fd(2, "VISITOR id=%d: unexpected server response\n", id);
        return 2;
    }
}

int main(int argc, char **argv) {
    if (argc == 2 && !strcmp(argv[1], "--help")) { usage(); return 0; }
    const char *host = NULL, *port = NULL;
    int id = 0, arrival = 0, port_number;
    static const struct option options[] = {
        {"host", required_argument, NULL, 'h'}, {"port", required_argument, NULL, 'p'},
        {"id", required_argument, NULL, 'i'}, {"arrival-ms", required_argument, NULL, 'a'},
        {NULL, 0, NULL, 0}
    };
    int option, bad = 0;
    opterr = 0;
    while ((option = getopt_long(argc, argv, "", options, NULL)) != -1) {
        switch (option) {
            case 'h': host = optarg; break;
            case 'p': port = optarg; break;
            case 'i': bad |= parse_int(optarg, 1, MAX_VISITORS, &id); break;
            case 'a': bad |= parse_int(optarg, 0, 3600000, &arrival); break;
            default: bad = -1;
        }
    }
    if (bad || optind != argc || !host || !port || id == 0 || parse_int(port, 1, 65535, &port_number)) {
        usage(); print_fd(2, "Invalid arguments\n"); return 1;
    }
    ignore_sigpipe();
    if (sleep_ms(arrival)) { perror("nanosleep"); return 1; }
    int fd = connect_tcp(host, port);
    if (fd < 0) { perror("connect"); return 1; }
    int result = visit(fd, id);
    close(fd);
    return result;
}
