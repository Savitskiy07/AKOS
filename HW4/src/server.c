#define _POSIX_C_SOURCE 200809L
#include "common.h"

#include <assert.h>
#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define OUTPUT_LIMIT (256U * 1024U)
#define HANDSHAKE_MS 5000
#define DRAIN_MS 2000

typedef enum { UNKNOWN, VISITOR, OBSERVER } Role;
typedef enum { ABSENT, ENTRY_QUEUE, INSIDE, PICTURE_QUEUE, VIEWING, FINISHED, ABORTED } State;

typedef struct {
    int fd;
    Role role;
    int visitor;
    char input[LINE_SIZE];
    size_t input_len;
    char *output;
    size_t output_len, output_pos, output_capacity;
    bool closing;
    const char *drop_reason;
    int64_t accepted_at;
} Connection;

typedef struct {
    State state;
    int connection;
    int picture;
    uint64_t seen;
    uint64_t ticket;
    int64_t view_ends;
    uint32_t rng;
} Visitor;

typedef struct {
    const char *host, *port;
    int total, capacity, pictures;
    int picture_capacity[MAX_PICTURES];
    int view_min, view_max;
    uint32_t seed;
    bool random_route;
} Config;

typedef struct {
    Config config;
    Connection connections[MAX_CONNECTIONS];
    Visitor visitors[MAX_VISITORS];
    int viewing[MAX_PICTURES];
    int listener, arrived, inside, queued, finished, aborted;
    uint64_t sequence, ticket;
    int64_t started, drain_until;
    bool stopping;
    int exit_code;
} Server;

static volatile sig_atomic_t interrupted = 0;
static void on_signal(int signal_number) { (void)signal_number; interrupted = 1; }

static const char *state_name(State state) {
    static const char *names[] = {"ABSENT", "ENTRY_QUEUE", "INSIDE", "PICTURE_QUEUE", "VIEWING", "FINISHED", "ABORTED"};
    return names[state];
}

static uint64_t all_pictures(int count) {
    return count == 64 ? UINT64_MAX : (UINT64_C(1) << count) - 1;
}

/* Each connection owns a bounded queue. A slow observer cannot block send(). */
static void queue_bytes(Connection *c, const char *data, size_t size) {
    if (c->fd < 0 || c->drop_reason) return;
    size_t pending = c->output_len - c->output_pos;
    if (pending + size > OUTPUT_LIMIT) {
        c->drop_reason = "slow_consumer";
        return;
    }
    if (c->output_pos) {
        memmove(c->output, c->output + c->output_pos, pending);
        c->output_len = pending;
        c->output_pos = 0;
    }
    if (pending + size > c->output_capacity) {
        size_t capacity = c->output_capacity ? c->output_capacity : 4096;
        while (capacity < pending + size) capacity *= 2;
        char *new_output = realloc(c->output, capacity);
        if (!new_output) {
            c->drop_reason = "allocation_failure";
            return;
        }
        c->output = new_output;
        c->output_capacity = capacity;
    }
    memcpy(c->output + pending, data, size);
    c->output_len += size;
}

static void queue_line(Connection *c, const char *format, ...) {
    char line[LINE_SIZE];
    va_list args;
    va_start(args, format);
    int n = vsnprintf(line, sizeof(line), format, args);
    va_end(args);
    assert(n >= 0 && (size_t)n < sizeof(line));
    queue_bytes(c, line, (size_t)n);
}

static void event(Server *s, const char *type, int visitor, int picture, const char *text) {
    char line[LINE_SIZE];
    uint64_t seen = visitor >= 0 ? s->visitors[visitor].seen : 0;
    int n = snprintf(line, sizeof(line),
        "EVENT seq=%" PRIu64 " t_ms=%" PRId64 " type=%s visitor=%d picture=%d "
        "inside=%d queued=%d finished=%d aborted=%d viewers=%d seen=%016" PRIx64 " | %s\n",
        ++s->sequence, monotonic_ms() - s->started, type, visitor + 1, picture + 1,
        s->inside, s->queued, s->finished, s->aborted,
        picture >= 0 ? s->viewing[picture] : 0, seen, text);
    assert(n > 0 && (size_t)n < sizeof(line));
    write_all(STDOUT_FILENO, line, (size_t)n);
    for (int i = 0; i < MAX_CONNECTIONS; ++i) {
        Connection *c = &s->connections[i];
        if (c->fd >= 0 && c->role == OBSERVER && !c->closing)
            queue_bytes(c, line, (size_t)n);
    }
}

static void snapshot(Server *s, Connection *c) {
    /* No other state transition can interleave: the server has one event loop. */
    queue_line(c, "SNAPSHOT_BEGIN seq=%" PRIu64 " total=%d capacity=%d pictures=%d arrived=%d inside=%d queued=%d finished=%d aborted=%d strategy=%s seed=%" PRIu32 "\n",
        s->sequence, s->config.total, s->config.capacity, s->config.pictures,
        s->arrived, s->inside, s->queued, s->finished, s->aborted,
        s->config.random_route ? "random" : "sequential", s->config.seed);
    for (int p = 0; p < s->config.pictures; ++p)
        queue_line(c, "PICTURE id=%d used=%d capacity=%d\n", p + 1,
            s->viewing[p], s->config.picture_capacity[p]);
    for (int i = 0; i < s->config.total; ++i) {
        Visitor *v = &s->visitors[i];
        if (v->state != ABSENT)
            queue_line(c, "VISITOR id=%d state=%s picture=%d seen=%016" PRIx64 "\n",
                i + 1, state_name(v->state), v->picture + 1, v->seen);
    }
    queue_line(c, "SNAPSHOT_END seq=%" PRIu64 "\n", s->sequence);
}

static void close_connection(Server *s, int index, const char *reason) {
    Connection *c = &s->connections[index];
    if (c->fd < 0) return;
    close(c->fd);
    c->fd = -1;
    free(c->output);
    c->output = NULL;
    c->output_len = c->output_pos = c->output_capacity = 0;
    if (c->role == OBSERVER) {
        print_fd(STDERR_FILENO, "NOTICE observer disconnected reason=%s\n", reason);
    } else if (c->role == VISITOR && c->visitor >= 0) {
        Visitor *v = &s->visitors[c->visitor];
        v->connection = -1;
        if (v->state != FINISHED && v->state != ABORTED) {
            int picture = v->picture;
            if (v->state == ENTRY_QUEUE) --s->queued;
            else --s->inside;
            if (v->state == VIEWING) --s->viewing[picture];
            v->state = ABORTED;
            v->picture = -1;
            ++s->aborted;
            if (!s->stopping)
                event(s, "ABORT", c->visitor, picture, "Связь с посетителем потеряна; занятые места освобождены");
        }
    }
}

static void protocol_error(Connection *c, const char *reason) {
    queue_line(c, "ERROR %s\n", reason);
    c->closing = true;
}

static int split(char *line, char **tokens, int capacity) {
    int count = 0;
    char *save = NULL;
    for (char *p = strtok_r(line, " ", &save); p; p = strtok_r(NULL, " ", &save)) {
        if (count == capacity) return -1;
        tokens[count++] = p;
    }
    return count;
}

static void command(Server *s, int index, char *line) {
    Connection *c = &s->connections[index];
    char *tokens[4];
    int count = split(line, tokens, 4);
    if (count <= 0) { protocol_error(c, "invalid_command"); return; }
    if (c->role == UNKNOWN) {
        if (count == 2 && strcmp(tokens[0], "HELLO") == 0 && strcmp(tokens[1], "OBSERVER") == 0) {
            c->role = OBSERVER;
            snapshot(s, c);
            print_fd(STDERR_FILENO, "NOTICE observer connected snapshot_seq=%" PRIu64 "\n", s->sequence);
            return;
        }
        int id;
        if (count != 3 || strcmp(tokens[0], "HELLO") || strcmp(tokens[1], "VISITOR") ||
            parse_int(tokens[2], 1, s->config.total, &id)) {
            protocol_error(c, "expected_HELLO_VISITOR_id_or_HELLO_OBSERVER");
            return;
        }
        Visitor *v = &s->visitors[id - 1];
        if (v->state != ABSENT) { protocol_error(c, "visitor_id_already_used"); return; }
        c->role = VISITOR;
        c->visitor = id - 1;
        v->connection = index;
        v->state = ENTRY_QUEUE;
        v->ticket = ++s->ticket;
        v->rng = s->config.seed ^ ((uint32_t)id * UINT32_C(2654435761));
        ++s->arrived;
        ++s->queued;
        queue_line(c, "CONFIG %d %s %" PRIu32 "\n", s->config.pictures,
            s->config.random_route ? "random" : "sequential", v->rng);
        queue_line(c, "WAIT_ENTRY\n");
        event(s, "ARRIVAL", id - 1, -1, "Посетитель прибыл к галерее");
        if (s->inside >= s->config.capacity || s->queued > 1)
            event(s, "WAIT_ENTRY", id - 1, -1, "Посетитель ожидает разрешения вахтёра в очереди");
        if (s->arrived == s->config.total)
            event(s, "INPUT_CLOSED", -1, -1, "Все запланированные посетители прибыли; новые посетители не принимаются");
        return;
    }
    if (c->role == OBSERVER) { protocol_error(c, "observer_is_read_only"); return; }
    Visitor *v = &s->visitors[c->visitor];
    if (count == 2 && strcmp(tokens[0], "VIEW") == 0) {
        int p;
        if (v->state != INSIDE || parse_int(tokens[1], 1, s->config.pictures, &p)) {
            protocol_error(c, "invalid_VIEW_state_or_picture");
            return;
        }
        --p;
        if (v->seen & (UINT64_C(1) << p)) { protocol_error(c, "picture_already_seen"); return; }
        v->state = PICTURE_QUEUE;
        v->picture = p;
        v->ticket = ++s->ticket;
        event(s, "CHOOSE", c->visitor, p, "Посетитель выбрал следующую картину");
        int waiting = 0;
        for (int i = 0; i < s->config.total; ++i)
            if (s->visitors[i].state == PICTURE_QUEUE && s->visitors[i].picture == p) ++waiting;
        if (s->viewing[p] + waiting > s->config.picture_capacity[p]) {
            queue_line(c, "WAIT_PICTURE %d\n", p + 1);
            event(s, "WAIT_PICTURE", c->visitor, p, "Посетитель ожидает свободного места возле картины");
        }
    } else if (count == 1 && strcmp(tokens[0], "LEAVE") == 0) {
        if (v->state != INSIDE || v->seen != all_pictures(s->config.pictures)) {
            protocol_error(c, "all_pictures_must_be_seen_before_LEAVE");
            return;
        }
        v->state = FINISHED;
        --s->inside;
        ++s->finished;
        event(s, "EXIT", c->visitor, -1, "Посетитель осмотрел все картины и покинул галерею");
        queue_line(c, "BYE\n");
        c->closing = true;
    } else {
        protocol_error(c, "unexpected_command");
    }
}

static void read_connection(Server *s, int index) {
    Connection *c = &s->connections[index];
    /* Bound work per poll turn so a noisy connection cannot starve timers. */
    for (int batch = 0; batch < 16 && !c->closing && !c->drop_reason; ++batch) {
        char data[4096];
        ssize_t n = recv(c->fd, data, sizeof(data), 0);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
        if (n <= 0) { c->drop_reason = n == 0 ? "peer_closed" : "recv_error"; return; }
        for (ssize_t j = 0; j < n && !c->closing && !c->drop_reason; ++j) {
            unsigned char ch = (unsigned char)data[j];
            if (ch == '\n') {
                c->input[c->input_len] = '\0';
                command(s, index, c->input);
                c->input_len = 0;
            } else if (ch < 32 || ch > 126 || c->input_len + 1 >= sizeof(c->input)) {
                protocol_error(c, "invalid_or_oversized_line");
            } else {
                c->input[c->input_len++] = (char)ch;
            }
        }
    }
}

static void flush_connection(Connection *c) {
    for (int batch = 0; batch < 16 && c->output_pos < c->output_len; ++batch) {
        ssize_t n = send(c->fd, c->output + c->output_pos,
            c->output_len - c->output_pos, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
        if (n <= 0) { c->drop_reason = "send_error"; return; }
        c->output_pos += (size_t)n;
    }
    if (c->output_pos == c->output_len) c->output_pos = c->output_len = 0;
}

static void accept_connections(Server *s) {
    for (int batch = 0; batch < 32; ++batch) {
        int fd = accept(s->listener, NULL, NULL);
        if (fd < 0 && errno == EINTR) continue;
        if (fd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK)
                print_fd(STDERR_FILENO, "accept: %s\n", strerror(errno));
            return;
        }
        int i;
        for (i = 0; i < MAX_CONNECTIONS; ++i) if (s->connections[i].fd < 0) break;
        if (i == MAX_CONNECTIONS || set_nonblocking(fd)) { close(fd); continue; }
        /* Small kernel send buffers also bound memory spent on idle observers. */
        int buffer_size = 16384;
        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size));
        int no_delay = 1;
        if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &no_delay, sizeof(no_delay)) < 0) {
            close(fd);
            continue;
        }
        Connection *c = &s->connections[i];
        memset(c, 0, sizeof(*c));
        c->fd = fd;
        c->visitor = -1;
        c->accepted_at = monotonic_ms();
    }
}

static int oldest_waiter(Server *s, State state, int picture) {
    int result = -1;
    uint64_t ticket = UINT64_MAX;
    for (int i = 0; i < s->config.total; ++i) {
        Visitor *v = &s->visitors[i];
        if (v->state == state && (picture < 0 || v->picture == picture) && v->ticket < ticket &&
            v->connection >= 0 && !s->connections[v->connection].closing &&
            !s->connections[v->connection].drop_reason) {
            result = i;
            ticket = v->ticket;
        }
    }
    return result;
}

static void check_invariants(Server *s) {
    int inside = 0, queued = 0, finished = 0, aborted = 0, arrived = 0;
    int viewing[MAX_PICTURES] = {0};
    for (int i = 0; i < s->config.total; ++i) {
        Visitor *v = &s->visitors[i];
        if (v->state != ABSENT) ++arrived;
        if (v->state == ENTRY_QUEUE) ++queued;
        if (v->state == FINISHED) {
            ++finished;
            assert(v->seen == all_pictures(s->config.pictures));
        }
        if (v->state == ABORTED) ++aborted;
        if (v->state == INSIDE || v->state == PICTURE_QUEUE || v->state == VIEWING) ++inside;
        if (v->state == VIEWING || v->state == PICTURE_QUEUE) {
            assert(v->picture >= 0 && v->picture < s->config.pictures);
            assert(!(v->seen & (UINT64_C(1) << v->picture)));
        } else assert(v->picture == -1);
        if (v->state == VIEWING) ++viewing[v->picture];
        assert((v->seen & ~all_pictures(s->config.pictures)) == 0);
    }
    assert(inside == s->inside && inside <= s->config.capacity);
    assert(queued == s->queued && finished == s->finished && aborted == s->aborted);
    assert(arrived == s->arrived && arrived == inside + queued + finished + aborted);
    for (int p = 0; p < s->config.pictures; ++p)
        assert(viewing[p] == s->viewing[p] && viewing[p] <= s->config.picture_capacity[p]);
}

static void advance_model(Server *s) {
    int64_t now = monotonic_ms();
    for (int i = 0; i < s->config.total; ++i) {
        Visitor *v = &s->visitors[i];
        if (v->state == VIEWING && v->view_ends <= now &&
            !s->connections[v->connection].closing && !s->connections[v->connection].drop_reason) {
            int p = v->picture;
            --s->viewing[p];
            v->seen |= UINT64_C(1) << p;
            v->state = INSIDE;
            v->picture = -1;
            queue_line(&s->connections[v->connection], "DONE %d\n", p + 1);
            event(s, "VIEW_END", i, p, "Посетитель закончил осмотр картины; место освобождено");
        }
    }
    while (s->inside < s->config.capacity) {
        int id = oldest_waiter(s, ENTRY_QUEUE, -1);
        if (id < 0) break;
        Visitor *v = &s->visitors[id];
        event(s, "PERMIT", id, -1, "Вахтёр разрешил вход посетителю");
        v->state = INSIDE;
        --s->queued;
        ++s->inside;
        queue_line(&s->connections[v->connection], "ENTERED\n");
        event(s, "ENTER", id, -1, "Посетитель вошёл в галерею");
    }
    for (int p = 0; p < s->config.pictures; ++p) {
        while (s->viewing[p] < s->config.picture_capacity[p]) {
            int id = oldest_waiter(s, PICTURE_QUEUE, p);
            if (id < 0) break;
            Visitor *v = &s->visitors[id];
            int duration = random_between(&v->rng, s->config.view_min, s->config.view_max);
            v->state = VIEWING;
            ++s->viewing[p];
            v->view_ends = monotonic_ms() + duration;
            queue_line(&s->connections[v->connection], "START %d %d\n", p + 1, duration);
            event(s, "VIEW_START", id, p, "Посетитель начал осмотр картины");
        }
    }
    check_invariants(s);
}

static void begin_shutdown(Server *s, bool by_signal) {
    s->exit_code = by_signal ? 130 : (s->aborted ? 2 : 0);
    event(s, by_signal ? "INTERRUPTED" : "CLOSED", -1, -1,
        by_signal ? "Рабочий день прерван пользователем" : "Рабочий день завершён; в галерее никого нет");
    s->stopping = true;
    s->drain_until = monotonic_ms() + DRAIN_MS;
    close(s->listener);
    s->listener = -1;
    print_fd(STDOUT_FILENO, "SUMMARY total=%d arrived=%d finished=%d aborted=%d remaining=%d status=%s\n",
        s->config.total, s->arrived, s->finished, s->aborted,
        s->config.total - s->finished - s->aborted,
        by_signal ? "interrupted" : (s->aborted ? "incomplete" : "complete"));
    for (int i = 0; i < MAX_CONNECTIONS; ++i) {
        Connection *c = &s->connections[i];
        if (c->fd >= 0 && !c->closing) {
            queue_line(c, "STOP reason=%s\n", by_signal ? "interrupt" : (s->aborted ? "incomplete" : "complete"));
            c->closing = true;
        }
    }
}

static int poll_timeout(Server *s) {
    int64_t now = monotonic_ms(), next = now + 100;
    if (s->stopping && s->drain_until < next) next = s->drain_until;
    if (!s->stopping)
        for (int i = 0; i < s->config.total; ++i)
            if (s->visitors[i].state == VIEWING && s->visitors[i].view_ends < next)
                next = s->visitors[i].view_ends;
    return next <= now ? 0 : (int)(next - now);
}

static int run(Server *s) {
    struct pollfd descriptors[MAX_CONNECTIONS + 1];
    int indices[MAX_CONNECTIONS + 1];
    for (;;) {
        for (int i = 0; i < MAX_CONNECTIONS; ++i) {
            Connection *c = &s->connections[i];
            if (c->fd < 0) continue;
            if (c->drop_reason) close_connection(s, i, c->drop_reason);
            else if (c->closing && c->output_pos == c->output_len) close_connection(s, i, "flushed");
            else if (c->role == UNKNOWN && monotonic_ms() - c->accepted_at >= HANDSHAKE_MS)
                close_connection(s, i, "handshake_timeout");
        }
        if (!s->stopping) {
            advance_model(s);
            if (interrupted || s->finished + s->aborted == s->config.total)
                begin_shutdown(s, interrupted != 0);
        }
        int count = 0;
        if (s->listener >= 0) {
            descriptors[count] = (struct pollfd){.fd = s->listener, .events = POLLIN};
            indices[count++] = -1;
        }
        for (int i = 0; i < MAX_CONNECTIONS; ++i) {
            Connection *c = &s->connections[i];
            if (c->fd < 0 || c->drop_reason) continue;
            short events = c->closing ? 0 : POLLIN;
            if (c->output_pos < c->output_len) events |= POLLOUT;
            descriptors[count] = (struct pollfd){.fd = c->fd, .events = events};
            indices[count++] = i;
        }
        if (s->stopping && (count == 0 || monotonic_ms() >= s->drain_until)) break;
        int ready = poll(descriptors, (nfds_t)count, poll_timeout(s));
        if (ready < 0 && errno == EINTR) continue;
        if (ready < 0) { perror("poll"); return 1; }
        for (int j = 0; j < count; ++j) {
            short flags = descriptors[j].revents;
            if (!flags) continue;
            if (indices[j] < 0) {
                if (flags & POLLIN) accept_connections(s);
                if (flags & (POLLERR | POLLNVAL)) { print_fd(2, "listener poll error\n"); return 1; }
                continue;
            }
            int i = indices[j];
            Connection *c = &s->connections[i];
            if (!c->closing && (flags & (POLLIN | POLLHUP))) read_connection(s, i);
            if (!c->drop_reason && (flags & POLLOUT)) flush_connection(c);
            if (flags & (POLLERR | POLLNVAL)) c->drop_reason = "poll_error";
            else if ((flags & POLLHUP) && c->closing) c->drop_reason = "peer_closed";
        }
    }
    return s->exit_code;
}

static void usage(void) {
    print_fd(1, "Usage: gallery_server --host IP --port PORT [options]\n"
        "  --visitors N             0..1000, default 100; visitor IDs are 1..N\n"
        "  --capacity N             1..1000, default 10\n"
        "  --pictures N             1..64, default 5\n"
        "  --picture-capacity N     1..1000, default 2\n"
        "  --picture-capacities A,B  one positive capacity per picture; overrides uniform value\n"
        "  --view-min-ms N          0..60000, default 200\n"
        "  --view-max-ms N          0..60000, default 500\n"
        "  --strategy NAME          random (default) or sequential\n"
        "  --seed N                 0..4294967295, default 1\n");
}

static int configure(int argc, char **argv, Config *c) {
    *c = (Config){.total = 100, .capacity = 10, .pictures = 5,
        .view_min = 200, .view_max = 500, .seed = 1, .random_route = true};
    const char *capacities = NULL;
    int uniform_capacity = 2, port_number;
    static const struct option options[] = {
        {"host", required_argument, NULL, 'h'}, {"port", required_argument, NULL, 'p'},
        {"visitors", required_argument, NULL, 'n'}, {"capacity", required_argument, NULL, 'c'},
        {"pictures", required_argument, NULL, 'k'}, {"picture-capacity", required_argument, NULL, 'l'},
        {"picture-capacities", required_argument, NULL, 'L'},
        {"view-min-ms", required_argument, NULL, 'a'}, {"view-max-ms", required_argument, NULL, 'b'},
        {"strategy", required_argument, NULL, 's'}, {"seed", required_argument, NULL, 'r'},
        {"help", no_argument, NULL, '?'}, {NULL, 0, NULL, 0}
    };
    int option;
    opterr = 0;
    while ((option = getopt_long(argc, argv, "", options, NULL)) != -1) {
        int bad = 0;
        switch (option) {
            case 'h': c->host = optarg; break;
            case 'p': c->port = optarg; break;
            case 'n': bad = parse_int(optarg, 0, MAX_VISITORS, &c->total); break;
            case 'c': bad = parse_int(optarg, 1, MAX_VISITORS, &c->capacity); break;
            case 'k': bad = parse_int(optarg, 1, MAX_PICTURES, &c->pictures); break;
            case 'l': bad = parse_int(optarg, 1, MAX_VISITORS, &uniform_capacity); break;
            case 'L': capacities = optarg; break;
            case 'a': bad = parse_int(optarg, 0, 60000, &c->view_min); break;
            case 'b': bad = parse_int(optarg, 0, 60000, &c->view_max); break;
            case 'r': bad = parse_u32(optarg, &c->seed); break;
            case 's':
                if (!strcmp(optarg, "random")) c->random_route = true;
                else if (!strcmp(optarg, "sequential")) c->random_route = false;
                else bad = -1;
                break;
            default: return -1;
        }
        if (bad) return -1;
    }
    if (optind != argc || !c->host || !c->port || parse_int(c->port, 1, 65535, &port_number) ||
        c->view_min > c->view_max) return -1;
    for (int p = 0; p < c->pictures; ++p) c->picture_capacity[p] = uniform_capacity;
    if (capacities) {
        char *copy = strdup(capacities);
        if (!copy) return -1;
        char *current = copy;
        int count = 0, result = 0;
        for (;;) {
            char *comma = strchr(current, ',');
            if (comma) *comma = '\0';
            if (count >= c->pictures || parse_int(current, 1, MAX_VISITORS, &c->picture_capacity[count])) {
                result = -1;
                break;
            }
            ++count;
            if (!comma) break;
            current = comma + 1;
        }
        free(copy);
        if (result || count != c->pictures) return -1;
    }
    return 0;
}

int main(int argc, char **argv) {
    if (argc == 2 && !strcmp(argv[1], "--help")) { usage(); return 0; }
    Config config;
    if (configure(argc, argv, &config)) { usage(); print_fd(2, "Invalid arguments\n"); return 1; }
    ignore_sigpipe();
    struct sigaction action = {0};
    action.sa_handler = on_signal;
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);
    Server *s = calloc(1, sizeof(*s));
    if (!s) { perror("calloc"); return 1; }
    s->config = config;
    s->started = monotonic_ms();
    for (int i = 0; i < MAX_CONNECTIONS; ++i) s->connections[i].fd = -1;
    for (int i = 0; i < MAX_VISITORS; ++i) {
        s->visitors[i].connection = -1;
        s->visitors[i].picture = -1;
    }
    s->listener = listen_tcp(config.host, config.port);
    if (s->listener < 0 || set_nonblocking(s->listener)) {
        perror("listen");
        if (s->listener >= 0) close(s->listener);
        free(s);
        return 1;
    }
    print_fd(1, "READY host=%s port=%s visitors=%d capacity=%d pictures=%d seed=%" PRIu32 "\n",
        config.host, config.port, config.total, config.capacity, config.pictures, config.seed);
    event(s, "OPEN", -1, -1, "Галерея открыта; вахтёр ожидает посетителей");
    int result = run(s);
    s->stopping = true;
    if (s->listener >= 0) close(s->listener);
    for (int i = 0; i < MAX_CONNECTIONS; ++i) close_connection(s, i, "server_shutdown");
    free(s);
    return result;
}
