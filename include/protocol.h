#pragma once
#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <string>
#include <sys/socket.h>
#include <utility>

namespace tcp {
constexpr uint32_t kMaxMessageSize = 1024 * 1024; // 1 MiB

inline bool send_all(int fd, const void* data, size_t len)
{
    const char* p = static_cast<const char*>(data);
    size_t done = 0;
    while (done < len) {
        const ssize_t n = send(fd, p + done, len - done, MSG_NOSIGNAL);
        if (n > 0) { done += static_cast<size_t>(n); continue; }
        if (n < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

inline bool recv_all(int fd, void* data, size_t len)
{
    char* p = static_cast<char*>(data);
    size_t done = 0;
    while (done < len) {
        const ssize_t n = recv(fd, p + done, len - done, 0);
        if (n > 0) { done += static_cast<size_t>(n); continue; }
        if (n == 0) return false; // Peer closed before all bytes arrived.
        if (errno == EINTR) continue;
        return false;
    }
    return true;
}

// Frame format: 4-byte unsigned length in network byte order + message bytes.
inline bool send_message(int fd, const std::string& message)
{
    if (message.size() > kMaxMessageSize) { errno = EMSGSIZE; return false; }
    const uint32_t network_length = htonl(static_cast<uint32_t>(message.size()));
    if (!send_all(fd, &network_length, sizeof(network_length))) return false;
    return message.empty() || send_all(fd, message.data(), message.size());
}

inline bool recv_message(int fd, std::string& message)
{
    uint32_t network_length = 0;
    if (!recv_all(fd, &network_length, sizeof(network_length))) return false;
    const uint32_t length = ntohl(network_length);
    if (length > kMaxMessageSize) { errno = EMSGSIZE; return false; }
    std::string received(length, '\0');
    if (length > 0 && !recv_all(fd, received.data(), length)) return false;
    message = std::move(received);
    return true;
}
} // namespace tcp
