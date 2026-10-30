#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

using Clock = std::chrono::steady_clock;

bool send_all(int fd, const void* data, std::size_t len) {
    const char* p = static_cast<const char*>(data);
    while (len > 0) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        p += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

bool recv_all(int fd, void* data, std::size_t len) {
    char* p = static_cast<char*>(data);
    while (len > 0) {
        ssize_t n = recv(fd, p, len, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        p += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

bool send_frame(int fd, const std::string& msg) {
    uint32_t size_be = htonl(static_cast<uint32_t>(msg.size()));
    return send_all(fd, &size_be, sizeof(size_be)) &&
           send_all(fd, msg.data(), msg.size());
}

bool recv_frame(int fd, std::string& msg) {
    uint32_t size_be = 0;
    if (!recv_all(fd, &size_be, sizeof(size_be))) return false;
    uint32_t size = ntohl(size_be);
    if (size == 0 || size > 4096) return false;
    msg.resize(size);
    return recv_all(fd, msg.data(), size);
}

struct Result {
    int acknowledged = 0;
    std::vector<double> latencies_ms;
    std::string error;
};
double percentile(std::vector<double> sorted, double p) {
    if (sorted.empty()) return 0.0;
    std::sort(sorted.begin(), sorted.end());
    std::size_t index = static_cast<std::size_t>(std::ceil(p * sorted.size())) - 1;
    return sorted[index];
}

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "Usage: ./day11_bench <clients 1..4> <messages_per_client> <tcp_nodelay 0|1>\n";
        return 1;
    }
    int clients = 0, per_client = 0, nodelay = 0;
    try {
        clients = std::stoi(argv[1]);
        per_client = std::stoi(argv[2]);
        nodelay = std::stoi(argv[3]);
    } catch (...) {
        std::cerr << "Arguments must be integers\n";
        return 1;
    }
    if (clients < 1 || clients > 4 || per_client < 1 || per_client > 100000 || (nodelay != 0 && nodelay != 1)) {
        std::cerr << "Invalid arguments\n";
        return 1;
    }

    const auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const std::string run_id = "d10_" + std::to_string(now_us);
    std::vector<Result> results(clients);
    std::vector<std::thread> threads;
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};

    for (int i = 0; i < clients; ++i) {
        threads.emplace_back([&, i]() {
            ready.fetch_add(1);
            while (!go.load()) std::this_thread::yield();

            int fd = socket(AF_INET, SOCK_STREAM, 0);
            if (fd < 0) { results[i].error = "socket failed"; return; }
            timeval tv{};
            tv.tv_sec = 3;
            if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0 ||
                setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
                results[i].error = "setsockopt failed";
                close(fd);
                return;
            }
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(9000);
            inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
            if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
                results[i].error = std::string("connect: ") + std::strerror(errno);
                close(fd);
                return;
            }

            if (nodelay == 1) {
                const int enabled = 1;
                if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
                               &enabled, sizeof(enabled)) != 0) {
                    results[i].error = std::string("TCP_NODELAY: ") + std::strerror(errno);
                    close(fd);
                    return;
                }
            }

            for (int j = 0; j < per_client; ++j) {
                const std::string device = "day10dev" + std::to_string(i);
                const std::string event = run_id + "_t" + std::to_string(i)
                                           + "_n" + std::to_string(j);
                const auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                const std::string msg = device + "|" + event + "|" +
                    std::to_string(j + 1) + "|" + std::to_string(ts) + "|26.1";
                const auto begin = Clock::now();
                std::string ack;
                if (!send_frame(fd, msg) || !recv_frame(fd, ack)) {
                    results[i].error = "send/recv failed at message " + std::to_string(j);
                    break;
                }
                if (ack != "GATEWAY_STORED") {
                    results[i].error = "unexpected ACK: " + ack;
                    break;
                }
                const double ms = std::chrono::duration<double, std::milli>(
                    Clock::now() - begin).count();
                results[i].latencies_ms.push_back(ms);
                ++results[i].acknowledged;
            }
            close(fd);
        });
    }
    while (ready.load() != clients) std::this_thread::yield();
    const auto begin = Clock::now();
    go.store(true);
    for (auto& thread : threads) thread.join();
    const double seconds = std::chrono::duration<double>(Clock::now() - begin).count();

    int succeeded = 0;
    std::vector<double> all;
    for (int i = 0; i < clients; ++i) {
        succeeded += results[i].acknowledged;
        all.insert(all.end(), results[i].latencies_ms.begin(), results[i].latencies_ms.end());
        if (!results[i].error.empty())
            std::cout << "worker " << i << " error: " << results[i].error << '\n';
    }
    std::cout << "tcp_nodelay=" << (nodelay ? "on" : "off") << '\n'
              << "run_id=" << run_id << '\n'
              << "clients=" << clients << " planned=" << clients * per_client
              << " acknowledged=" << succeeded << '\n'
              << "elapsed_s=" << seconds << '\n'
              << "throughput_ack_per_s=" << succeeded / seconds << '\n'
              << "ack_latency_p50_ms=" << percentile(all, 0.50) << '\n'
              << "ack_latency_p95_ms=" << percentile(all, 0.95) << '\n';
    return succeeded == clients * per_client ? 0 : 2;
}
