/* Low-rate continuous UDP probing using the existing sockperf wire format. */
#include <chrono>
#include <deque>
#include <poll.h>

namespace continuous {
using Clock = std::chrono::steady_clock;
using Micros = std::chrono::microseconds;

struct Probe {
    uint64_t sequence;
    Clock::time_point sent;
    long long wall;
    long long rtt;
};

static volatile sig_atomic_t stopped = 0;

static void stop(int) { stopped = 1; }

static int open_socket(const char *host, const char *port) {
    addrinfo hints = {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_NUMERICSERV;
    addrinfo *addresses = nullptr;
    if (getaddrinfo(host, port, &hints, &addresses) != 0)
        return -1;
    int fd = -1;
    for (auto addr = addresses; addr; addr = addr->ai_next) {
        fd = socket(addr->ai_family, SOCK_DGRAM | SOCK_NONBLOCK, addr->ai_protocol);
        if (fd >= 0 && connect(fd, addr->ai_addr, addr->ai_addrlen) == 0)
            break;
        if (fd >= 0)
            close(fd);
        fd = -1;
    }
    freeaddrinfo(addresses);
    return fd;
}

static long number(const char *value, long minimum, long maximum) {
    char *end;
    long parsed = strtol(value, &end, 10);
    return end != value && !*end && parsed >= minimum && parsed <= maximum ? parsed : 0;
}

static int run(int argc, char **argv) {
    if (argc == 3 && !strcmp(argv[2], "--help")) {
        puts("sockperf continuous HOST PORT [INTERVAL_MS=500] [TIMEOUT_MS=2000] [BYTES=64]");
        return 0;
    }
    if (argc < 4 || argc > 7)
        return 2;
    const long interval = argc > 4 ? number(argv[4], 10, 60000) : 500;
    const long timeout = argc > 5 ? number(argv[5], 10, 60000) : 2000;
    const long bytes = argc > 6 ? number(argv[6], MsgHeader::EFFECTIVE_SIZE, 1200) : 64;
    if (!interval || !timeout || !bytes || !number(argv[3], 1, 65535))
        return 2;

    struct sigaction action = {};
    action.sa_handler = stop;
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, nullptr);
    sigaction(SIGTERM, &action, nullptr);

    Message::initMaxSize(bytes);
    Message::initMaxSeqNo(UINT64_MAX);
    Message request, reply;
    memset(request.getBuf(), 0, bytes);
    request.setClient();
    request.getHeader()->setPongRequest();
    request.setLength(bytes);

    std::deque<Probe> pending;
    auto next_send = Clock::now();
    int fd = -1;
    unsigned failures = 0;
    uint64_t sequence = 0;
    while (!stopped || !pending.empty()) {
        auto now = Clock::now();
        while (!pending.empty() &&
               (pending.front().rtt >= 0 || now >= pending.front().sent + std::chrono::milliseconds(timeout))) {
            const auto probe = pending.front();
            pending.pop_front();
            failures = probe.rtt < 0 ? failures + 1 : 0;
            if (printf("%lld,%llu,%lld\n", probe.wall,
                       static_cast<unsigned long long>(probe.sequence), probe.rtt) < 0 || fflush(stdout))
                return 1;
        }

        if (!stopped && now >= next_send) {
            if (failures >= 4 && fd >= 0) {
                close(fd);
                fd = -1;
            }
            if (fd < 0) {
                fd = open_socket(argv[2], argv[3]);
                failures = 0;
            }
            now = Clock::now();
            const auto wall = std::chrono::duration_cast<Micros>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            request.setSequenceCounter(++sequence);
            request.setHeaderToNetwork();
            if (fd >= 0)
                send(fd, request.getBuf(), bytes, 0);
            request.setHeaderToHost();
            pending.push_back({sequence, now, wall, -1});
            next_send = now + std::chrono::milliseconds(interval);
        }
        if (stopped && pending.empty())
            break;

        auto wake = stopped ? Clock::time_point::max() : next_send;
        if (!pending.empty())
            wake = std::min(wake, pending.front().sent + std::chrono::milliseconds(timeout));
        pollfd input = {fd, POLLIN, 0};
        const auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(wake - Clock::now()).count();
        if (poll(&input, 1, static_cast<int>(std::max<long long>(0, delay + 1))) <= 0)
            continue;
        const auto length = recv(fd, reply.getBuf(), bytes, 0);
        const auto received = Clock::now();
        if (length != bytes)
            continue;
        reply.setHeaderToHost();
        if (reply.isClient() || !reply.isPongRequest() || reply.getLength() != bytes)
            continue;
        const auto seq = reply.getSequenceCounter();
        for (auto &probe : pending) {
            if (probe.sequence == seq && probe.rtt < 0 &&
                received <= probe.sent + std::chrono::milliseconds(timeout)) {
                probe.rtt = std::chrono::duration_cast<Micros>(received - probe.sent).count();
                break;
            }
        }
    }
    if (fd >= 0)
        close(fd);
    return 0;
}
}
